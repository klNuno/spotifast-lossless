#!/usr/bin/bash
# Builds the QEMU that runs Spotifast's Soloist machine on Windows: one
# static qemu-system-x86_64.exe with only the x86 PC, QEMU's own code
# translator, Windows' hypervisor platform and the user network, plus the
# three firmware files the PC machine loads. Run it under MSYS2's UCRT64
# shell with these packages:
#
#   pacman -S --needed mingw-w64-ucrt-x86_64-{gcc,glib2,pixman,libslirp,dtc,zlib,pkgconf,python,ninja,meson} make tar xz
#
#   soloist/vm/build-qemu.sh OUT_DIR
#
# The result goes in OUT_DIR next to what build-bundle.sh makes; the
# Windows installer ships that folder as vm\.
set -euo pipefail
out=${1:?usage: build-qemu.sh OUT_DIR}
mkdir -p "$out"
out=$(cd "$out" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"

VERSION=11.1.0
curl -sSfL -o qemu.tar.xz "https://download.qemu.org/qemu-$VERSION.tar.xz"
echo "6ee1d1a61f68212476b27108c26da5f449dc09b626d42f8279ba0dc2e08fa858  qemu.tar.xz" | sha256sum -c --quiet
tar xf qemu.tar.xz
cd "qemu-$VERSION"
mkdir build && cd build
../configure --target-list=x86_64-softmmu --without-default-features --static \
    --enable-tcg --enable-whpx --enable-slirp --enable-pixman \
    --disable-docs --disable-tools --disable-guest-agent --disable-werror \
    --extra-cflags=-Os >configure.log 2>&1 || { tail -30 configure.log; exit 1; }
ninja qemu-system-x86_64.exe >build.log 2>&1 || { grep -E 'error|FAILED' build.log | head -20; exit 1; }
strip -o "$out/qemu-system-x86_64.exe" qemu-system-x86_64.exe
cd ..
cp pc-bios/bios-256k.bin pc-bios/linuxboot_dma.bin pc-bios/kvmvapic.bin "$out/"
cp COPYING "$out/COPYING.QEMU"
cd "$out" && sha256sum qemu-system-x86_64.exe bios-256k.bin linuxboot_dma.bin kvmvapic.bin
