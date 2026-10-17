#[path = "src/env_config.rs"]
mod env_config;

fn main() {
    let env_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.env");
    println!("cargo:rerun-if-changed={}", env_path.display());
    println!("cargo:rerun-if-changed=src/env_config.rs");

    let values = match std::fs::File::open(&env_path) {
        Ok(file) => env_config::parse_s3_env(file).expect("无法读取项目根目录的 .env"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
        Err(_) => panic!("无法打开项目根目录的 .env"),
    };
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    // Only embed supported S3 settings, never unrelated .env secrets.
    std::fs::write(
        output.join("s3-config.json"),
        serde_json::to_vec(&values).unwrap(),
    )
    .expect("无法写入 S3 构建配置");

    tauri_build::build();
}
