use regex::Regex;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use std::sync::OnceLock;
use std::{env, str::FromStr};

static DB_NAME_RE: OnceLock<Regex> = OnceLock::new();

pub mod metadata;
pub mod search;
pub mod telemetry;

pub async fn create_pool() -> Result<PgPool, sqlx::Error> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");

    let opts = PgConnectOptions::from_str(&database_url).expect("Invalid DATABASE_URL");
    let db_name = opts.get_database().unwrap_or("postgres").to_string();

    let re = DB_NAME_RE.get_or_init(|| Regex::new(r"^[a-zA-Z0-9_]+$").unwrap());
    if !re.is_match(&db_name) {
        return Err(sqlx::Error::Configuration(
            format!("invalid database name: {db_name}").into(),
        ));
    }

    let probe = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts.clone())
        .await;

    match probe {
        Ok(p) => p.close().await,
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("3D000") => {
            let admin = match PgPoolOptions::new()
                .max_connections(1)
                .connect_with(opts.clone().database("postgres"))
                .await
            {
                Ok(p) => p,
                Err(_) => {
                    PgPoolOptions::new()
                        .max_connections(1)
                        .connect_with(opts.clone().database("template1"))
                        .await?
                }
            };
            let escaped = db_name.replace('"', "\"\"");
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE DATABASE \"{escaped}\""
            )))
            .execute(&admin)
            .await?;
            admin.close().await;
        }
        Err(e) => return Err(e),
    }

    let pool = PgPoolOptions::new()
        .max_connections(50)
        .connect_with(opts)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    Ok(pool)
}
