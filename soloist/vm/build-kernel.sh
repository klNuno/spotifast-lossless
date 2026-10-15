#!/bin/sh
# Builds the Linux kernel of Spotifast's Soloist virtual machine on Windows:
# Linux from kernel.org, unpatched, configured from tinyconfig plus
# kernel.config (beside this script), every driver built in.
#
#   build-kernel.sh OUT_DIR
#
# Needs curl, xz, make, gcc, flex, bison, bc and the libelf and OpenSSL
# headers. The tarball is pinned by SHA-256 (kernel.org's sha256sums.asc).
# Two builds of the same version give the same bzImage.
set -eu
out=${1:?usage: build-kernel.sh OUT_DIR}
fragment=$(cd "$(dirname "$0")" && pwd)/kernel.config
mkdir -p "$out"
out=$(cd "$out" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"

VERSION=6.12.112
SHA256=164dc9d1f6c93c61a15e1f071c48379b467f2b17c469cce7223471968208ed03
curl -sSfL -o linux.tar.xz "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-$VERSION.tar.xz"
echo "$SHA256  linux.tar.xz" | sha256sum -c --quiet
tar -xJf linux.tar.xz
rm linux.tar.xz
cd "linux-$VERSION"

# Fixed build stamps, so the image depends only on the source and the compiler.
export ARCH=x86_64 KBUILD_BUILD_TIMESTAMP='Thu Jan  1 00:00:00 UTC 1970' \
    KBUILD_BUILD_USER=spotifast KBUILD_BUILD_HOST=spotifast KBUILD_BUILD_VERSION=1
make -s tinyconfig
scripts/kconfig/merge_config.sh -m .config "$fragment" >/dev/null
make -s olddefconfig

# A missing dependency makes Kconfig drop a request without a word.
dropped=$(grep -E '^(# )?CONFIG_' "$fragment" | while IFS= read -r line; do
    case $line in
    "# "*)
        name=${line#\# }
        name=${name%% *}
        if grep -q "^$name=" .config; then
            echo "$line (now $(grep "^$name=" .config))"
        fi
        ;;
    *)
        if ! grep -qxF "$line" .config; then
            echo "$line (now $(grep -E "^(# )?${line%%=*}[= ]" .config || echo absent))"
        fi
        ;;
    esac
done)
if [ -n "$dropped" ]; then
    echo "Kconfig did not keep these requests:" >&2
    echo "$dropped" >&2
    exit 1
fi

make -s -j"${JOBS:-8}" bzImage
cp arch/x86/boot/bzImage "$out/vmlinuz"
size vmlinux | tail -1 | awk '{print "text " $1 " data " $2 " bss " $3}'
cd "$out" && sha256sum vmlinuz
