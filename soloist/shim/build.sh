#!/bin/sh
# Builds the two libraries Spotifast gives Spotify Soloist, for x86_64
# Linux: libpulse.so.0, the PulseAudio stand-in that hands its audio to
# Spotifast, and libatomic.so.1, so no system copy is needed. CI rebuilds
# them and compares the result with the committed libraries, so the two
# never drift.
#
#   soloist/shim/build.sh                  build both into soloist/shim/
#   PULSE_INCLUDE=/usr/include soloist/shim/build.sh
#                                          also check pa-abi.h against libpulse
#
# Reproducible output needs the same toolchain: Ubuntu 24.04's gcc 13 and
# binutils, as on GitHub's ubuntu-24.04 runners.
set -eu
cd "$(dirname "$0")"
CC=${CC:-gcc}
STRIP=${STRIP:-strip}
FLAGS="-std=gnu11 -O2 -fPIC -fvisibility=hidden -Wall -Wextra -Werror -ffile-prefix-map=$PWD=."
LINK="-shared -Wl,--build-id=none -Wl,-z,relro,-z,now"

if [ -n "${PULSE_INCLUDE:-}" ]; then
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    $CC -o "$tmp/real" -I"$PULSE_INCLUDE" check-abi.c
    $CC -o "$tmp/ours" -DOURS check-abi.c
    "$tmp/real" >"$tmp/real.txt"
    "$tmp/ours" >"$tmp/ours.txt"
    diff -u "$tmp/real.txt" "$tmp/ours.txt"
    echo "pa-abi.h matches libpulse ($(wc -l <"$tmp/real.txt") checks)"
fi

# shellcheck disable=SC2086
$CC $FLAGS $LINK -Wl,-soname,libpulse.so.0 -o libpulse.so.0 pulse-shim.c -lpthread -lm
# shellcheck disable=SC2086
$CC $FLAGS $LINK -Wl,-soname,libatomic.so.1 -Wl,--version-script=libatomic.map \
    -o libatomic.so.1 libatomic.c -lpthread
$STRIP --strip-unneeded libpulse.so.0 libatomic.so.1
sha256sum libpulse.so.0 libatomic.so.1
