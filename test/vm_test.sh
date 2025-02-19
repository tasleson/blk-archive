#!/usr/bin/bash

echo "Kernel version"
uname -a

echo "Distro info"
lsb_release -a

START_DIR=$(pwd)

echo "PWD $START_DIR"

# Name of modules we need for ublk_drv
K_PKG_NAME="linux-modules-extra-$(uname -r)"

# Install all the dependencies
export DEBIAN_FRONTEND="noninteractive"
apt-get update -y || exit 1

apt-get install git gcc clang-tools libdevmapper-dev pkg-config mount python3 python3-toml python3-pudb python3-numpy "$K_PKG_NAME"  -y || exit 1

# install rust via rustup as packages are too old on ubuntu
curl https://sh.rustup.rs -sSf | sh -s -- -y || exit 1

# Get rust tools in path
source "$HOME/.cargo/env" || exit 1

cargo build --release || exit 1

PATH=$PATH:$(pwd)/target/release
export path

if [ ! -d dmtest-python ]; then
    git clone https://github.com/jthornber/dmtest-python.git || exit 1
fi

# Create the block devices
truncate -s 1T /block1.img || exit 1
truncate -s 1T /block2.img || exit 1
truncate -s 1T /block3.img || exit 1

loop1=$(losetup -f --show /block1.img)
loop2=$(losetup -f --show /block2.img)
loop3=$(losetup -f --show /block3.img)


# Setup the ublk driver
modprobe ublk_drv
rc=$?
if [ $rc -ne 0 ]; then
    echo "Unable to load ublk_drv :-("
    exit 1
else
    git clone https://github.com/tasleson/test-bd || exit 1
    cd test-bd || exit 1
    cargo build --release || exit 1

    PATH=$PATH:$(pwd)/target/release
    export path

    # Create a combination of different sources
    test-bd add --seed 14266194193297977557 --id 0 || exit 1
    test-bd add -f 100 -d 0 -r 0 --seed 7705299977580309979 --id 1 || exit 1
    test-bd add -f 0 -d 100 -r 0 --seed 481508437336541701 --id 2 || exit 1
    test-bd add -f 0 -d 0 -r 100 --seed 12957482218230312183 --id 3 --size 256MiB || exit 1
    lsblk
fi

# Run the cargo based tests
cd "$START_DIR" || exit 1
cargo test || exit 1

# Run the dmtest-python tests for blk-archive
# Unable to run rolling linux test as we don't have enough disk space in the CI VMs.
cd "$START_DIR" || exit 1
# setup the configuration file for dmtest-python
cd dmtest-python || exit 1

echo "metadata_dev = '$loop1'" > config.toml
echo "data_dev = '$loop2'" >> config.toml
echo "disable_by_id_check = true" >> config.toml



export DMTEST_RESULT_SET=unit-test
./dmtest health || exit 1
./dmtest run blk-archive/unit/combinations
rc=$?
if [ $rc -ne 0 ]; then
    ./dmtest log /blk-archive/unit/combinations
    exit 1
fi
exit 0
