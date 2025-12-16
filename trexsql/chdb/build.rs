fn main() {
    // Tell cargo to look for libchdb in /usr/local/lib
    println!("cargo:rustc-link-search=native=/usr/local/lib");
}
