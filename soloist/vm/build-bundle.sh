#!/bin/sh
# Builds the Linux side of Spotifast's Soloist virtual machine on Windows:
# the kernel (build-kernel.sh) and base.cpio (busybox and glibc). Spotifast
# adds Soloist, the shim, the certificates and soloist/vm/init at each
# start, so this bundle changes only when the kernel or a package below does.
#
#   soloist/vm/build-bundle.sh OUT_DIR
#
# Needs curl, dpkg-deb, xz, GNU cpio (2.13 or newer) and what build-kernel.sh
# needs. Every package is pinned by SHA-256; Debian drops old versions from
# its pool, so a bump means new lines here, and snapshot.debian.org keeps
# the old files.
set -eu
out=${1:?usage: build-bundle.sh OUT_DIR}
mkdir -p "$out"
out=$(cd "$out" && pwd)
scripts=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"

POOL=https://deb.debian.org/debian/pool/main
fetch() {
    curl -sSfL -o "$1" "$POOL/$2"
    echo "$3  $1" | sha256sum -c --quiet
    dpkg-deb -x "$1" root
}
fetch busybox.deb b/busybox/busybox-static_1.37.0-6+b9_amd64.deb \
    598a3fd92bdafc34cd81196b2952ad36e910e47a4ecf162f8ce5e8e262598e53
fetch libc.deb g/glibc/libc6_2.41-12+deb13u4_amd64.deb \
    967aa62605721081c3eb2a17650611a792aa802d76a6511d1840242623d204c9

base=base
mkdir -p $base/bin $base/lib/x86_64-linux-gnu $base/lib64 $base/etc \
    $base/dev $base/proc $base/sys $base/tmp $base/run $base/data $base/opt/soloist
cp root/usr/bin/busybox $base/bin/busybox
# /init is a shell script; busybox installs the other applets itself.
ln -s busybox $base/bin/sh
for library in ld-linux-x86-64.so.2 libc.so.6 libm.so.6; do
    cp -L root/usr/lib/x86_64-linux-gnu/$library $base/lib/x86_64-linux-gnu/
done
ln -s /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 $base/lib64/ld-linux-x86-64.so.2
printf 'root:x:0:0:root:/:/bin/sh\n' >$base/etc/passwd
printf 'root:x:0:\n' >$base/etc/group
printf 'passwd: files\ngroup: files\nhosts: files dns\n' >$base/etc/nsswitch.conf
printf '127.0.0.1 localhost\n::1 localhost\n' >$base/etc/hosts
# QEMU's user network answers DNS on 10.0.2.3.
printf 'nameserver 10.0.2.3\n' >$base/etc/resolv.conf
chmod 1777 $base/tmp

find $base -exec touch -h -d @0 {} +
(cd $base && find . | LC_ALL=C sort | cpio -o -H newc -R 0:0 --reproducible --quiet) >"$out/base.cpio"
sh "$scripts/build-kernel.sh" "$out" >/dev/null
linux=$(sed -n 's/^VERSION=//p' "$scripts/build-kernel.sh")
cat >"$out/SOURCES.txt" <<EOF
Spotifast's Soloist virtual machine contains Linux $linux from kernel.org,
unmodified, configured by soloist/vm/kernel.config and built by
soloist/vm/build-kernel.sh: https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-$linux.tar.xz
and unmodified programs from Debian 13: busybox 1:1.37.0-6+b9, glibc 2.41-12+deb13u4.
Their sources: https://snapshot.debian.org/package/busybox/1:1.37.0-6/
https://snapshot.debian.org/package/glibc/2.41-12+deb13u4/
qemu-system-x86_64.exe is QEMU 11.1.0, unmodified, built by soloist/vm/build-qemu.sh,
with the firmware it ships (bios-256k.bin is SeaBIOS 1.17.0; linuxboot_dma.bin and
kvmvapic.bin are QEMU's own), under the GPL version 2 in COPYING.QEMU:
https://download.qemu.org/qemu-11.1.0.tar.xz
The scripts that build this machine, and its /init, are in Spotifast's source under soloist/vm/.
EOF
cd "$out" && sha256sum vmlinuz base.cpio
