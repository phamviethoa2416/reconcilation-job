package config

case class DatabaseConfig(
                           name: String,
                           jdbcUrl: String,
                           user: String,
                           password: String,
                           hdfsBasePath: String,
                         )