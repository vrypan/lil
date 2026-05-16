PKG_NAME := lil
VERSION := $(shell cargo metadata --no-deps --format-version 1 | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
DIST_DIR := dist
CARGO_PROFILE := dist

export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER ?= x86_64-linux-musl-gcc

MACOS_ARM64_TARGET := aarch64-apple-darwin
LINUX_X86_64_MUSL_TARGET := x86_64-unknown-linux-musl

MACOS_ARM64_BIN := target/$(MACOS_ARM64_TARGET)/$(CARGO_PROFILE)/$(PKG_NAME)
LINUX_X86_64_MUSL_BIN := target/$(LINUX_X86_64_MUSL_TARGET)/$(CARGO_PROFILE)/$(PKG_NAME)

MACOS_ARM64_ARCHIVE := $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(MACOS_ARM64_TARGET).tar.gz
LINUX_X86_64_MUSL_ARCHIVE := $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(LINUX_X86_64_MUSL_TARGET).tar.gz

.PHONY: all build dist macos-arm64 linux-x86_64-musl clean-dist

all: build

build: macos-arm64 linux-x86_64-musl

dist: $(MACOS_ARM64_ARCHIVE) $(LINUX_X86_64_MUSL_ARCHIVE)

macos-arm64:
	cargo build --profile $(CARGO_PROFILE) --target $(MACOS_ARM64_TARGET)

linux-x86_64-musl:
	cargo build --profile $(CARGO_PROFILE) --target $(LINUX_X86_64_MUSL_TARGET)

$(MACOS_ARM64_ARCHIVE): macos-arm64 README.md LICENSE | $(DIST_DIR)/.dir
	rm -rf $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(MACOS_ARM64_TARGET)
	mkdir -p $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(MACOS_ARM64_TARGET)
	cp $(MACOS_ARM64_BIN) README.md LICENSE $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(MACOS_ARM64_TARGET)/
	tar -czf $@ -C $(DIST_DIR) $(PKG_NAME)-$(VERSION)-$(MACOS_ARM64_TARGET)

$(LINUX_X86_64_MUSL_ARCHIVE): linux-x86_64-musl README.md LICENSE | $(DIST_DIR)/.dir
	rm -rf $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(LINUX_X86_64_MUSL_TARGET)
	mkdir -p $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(LINUX_X86_64_MUSL_TARGET)
	cp $(LINUX_X86_64_MUSL_BIN) README.md LICENSE $(DIST_DIR)/$(PKG_NAME)-$(VERSION)-$(LINUX_X86_64_MUSL_TARGET)/
	tar -czf $@ -C $(DIST_DIR) $(PKG_NAME)-$(VERSION)-$(LINUX_X86_64_MUSL_TARGET)

$(DIST_DIR)/.dir:
	mkdir -p $(DIST_DIR)
	touch $@

clean-dist:
	rm -rf $(DIST_DIR)
