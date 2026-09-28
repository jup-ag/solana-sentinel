FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

ARG RUST_TOOLCHAIN="1.91.1"
ARG SOLANA_CLI="3.0.14"

RUN sudo apt-get update && \
    sudo apt-get install -y \
        cmake \
        mold \
        gnupg \
        clang \
        libudev-dev \
        libsasl2-dev \
        libssl-dev \
        libzstd-dev \
        protobuf-compiler \
        pkg-config \
        libc-dev \
        clang \
        libclang-dev \
        llvm-dev

USER vscode
WORKDIR /home/vscode

SHELL ["/bin/zsh", "-c"]
ENV PATH="/home/vscode/.cargo/bin:$PATH"
ENV RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"
        
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    rustup toolchain install ${RUST_TOOLCHAIN} && \
    rustup default ${RUST_TOOLCHAIN}

RUN sh -c "$(curl -sSfL https://release.anza.xyz/v${SOLANA_CLI}/install)"

RUN echo 'PATH="/home/vscode/.local/bin:$PATH"' >> ~/.zshrc
