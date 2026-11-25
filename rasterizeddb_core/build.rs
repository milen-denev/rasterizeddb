//! Embeds the RasterizedDB icon (and version information) into the Windows
//! executable `rasterizeddb_core.exe`, so Explorer, the taskbar and Task Manager show the
//! mark. `compile_for` keeps it out of tests and benchmarks.
//!
//! The icon is a copy inside this crate (`assets/rasterizeddb.ico`, from
//! `branding/`), because a crate packaged for crates.io carries only its own
//! files. A missing icon or a resource-compiler failure is a build warning,
//! never a failed build: an icon is not worth a broken release.

const ICON: &str = "assets/rasterizeddb.ico";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={ICON}");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }
    if !std::path::Path::new(ICON).exists() {
        println!("cargo:warning=no {ICON}: rasterizeddb_core.exe is built without its icon");
        return;
    }
    let mut res = winresource::WindowsResource::new();
    res.set_icon(ICON)
        .set("FileDescription", "RasterizedDB server")
        .set("ProductName", "RasterizedDB");
    if let Err(e) = res.compile_for("rasterizeddb_core") {
        println!("cargo:warning=rasterizeddb_core.exe is built without its icon: {e}");
    }
}
