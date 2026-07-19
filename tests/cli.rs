use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn version_prints_package_version_without_app_server() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let isolated_root = std::env::temp_dir().join(format!(
        "spark-runner-version-{}-{unique}",
        std::process::id()
    ));
    let isolated_dir = isolated_root.join("bin");
    std::fs::create_dir_all(&isolated_dir).expect("create isolated binary directory");
    let isolated_exe = isolated_dir.join(format!("spark-runner{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(env!("CARGO_BIN_EXE_spark-runner"), &isolated_exe)
        .expect("copy spark-runner without its app-server sibling");

    let output = Command::new(&isolated_exe)
        .arg("--version")
        .output()
        .expect("run spark-runner --version");

    let _ = std::fs::remove_dir_all(&isolated_root);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("version stdout is UTF-8"),
        format!("spark-runner {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
}
