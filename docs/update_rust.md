When rustup says "Up to date : 1.27.1", it's referring to the rustup tool itself, not the Rust compiler or cargo versions. rustup version 1.27.1 is actually the latest version of the rustup tool.

To check and update your actual Rust compiler version, you can use:
```bash
rustc --version   # Check current rust version
rustup update stable  # Update to latest stable Rust
```

The reason for this separation is that rustup (the tool) and Rust (the language/compiler) are versioned independently. rustup is just the installer/version manager for Rust, similar to how npm is for Node.js or pip for Python.

If you want to see all installed and available Rust versions:
```bash
rustup show
```

To specifically update Rust to the latest stable version:
```bash
rustup toolchain install stable
rustup default stable
```

This will ensure you're using the latest stable version of Rust, regardless of your rustup version.