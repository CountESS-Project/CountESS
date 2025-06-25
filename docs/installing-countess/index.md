---
layout: default
---

# Installing CountESS

CountESS uses 'biobear', which is developed in Rust, so if you can't use a precompiled wheel
for biobear you'll need to install rust first, eg: `apt install rust-all` on Ubuntu.

## Linux

### ... using pip

CountESS can be [installed from pypi](https://pypi.org/project/countess/):

    pip install countess

... or install the latest development version directly from [github](https://github.com/):

    pip install git+https://github.com/CountESS-Project/CountESS.git

### ... using git, for development

Clone the repository and [install for development](https://pip.pypa.io/en/stable/cli/pip_install/#cmdoption-e):

    git clone https://github.com/CountESS-Project/CountESS.git
    cd CountESS
    pip install -e .[dev]

See [Contributing to CountESS](../contributing/) for more information on development.

### ... using nix

CountESS can be installed and run in one command with
[nix](https://nixos.org/):

    nix run github:CountESS-Project/CountESS

## MacOS

### ... using Homebrew

Even recent MacOS installs use a very old version of Python and Tk, so
we use [homebrew](https://brew.sh/) to install more recent versions.

1. download the homebrew tarball & make its binaries available from your shell.

   *These instructions are for installing on a Mac
   [without administrative privileges](https://docs.brew.sh/Installation#untar-anywhere-unsupported)
   If you already have homebrew installed you can skip this step.*

        cd
        mkdir homebrew
        curl -L https://github.com/Homebrew/brew/tarball/master | tar xz --strip 1 -C homebrew
        echo 'export PATH=$HOME/homebrew/bin:$PATH' >> .bash_profile
        source .bash_profile

2. Use homebrew to install a recent Tk library, Python 3.10 and rust
   into your `$HOME/homebrew/bin` directory.  This step is pretty slow:

        brew install tcl-tk python@3.10 python-tk@3.10 rust

   Optionally, add symlinks to your upgraded software for `python` and `pip`:

        cd
        mkdir bin
        ln -s $HOME/homebrew/bin/python3.10 bin/python
        ln -s $HOME/homebrew/bin/pip3.10 bin/pip
        echo 'export PATH=$HOME/bin:$PATH' >> .bash_profile
        source .bash_profile

3. Install countess using pip:

        pip3.10 install countess

   The CountESS binaries `countess_gui` and `countess_cli` should now be available from your shell.

4. Installing with HDF5 support

        brew install hdf5
        pip3.10 install countess[hdf5]


## Windows

* Install Python 3 from python.org

  https://docs.python.org/3/using/windows.html

  Under "Optional Features" select "py launcher" to make it easier to start python from the command shell.
  Under "Advanced Options" select "Add Python to environment variables"

* Install rust:

  * download `rustup-init.exe` from [rustup.rs](https://rustup.rs/)

  * Run `rustup-init.exe` to install rust and cargo.

  * If you haven't already got VS Code installed, select 1 to install it.

    * Ignore the prompt to log in to Visual Studio and just let rustup finish.
    * Once it is installed, run `rustup-init.exe` and select 1 again.
    * This time it should say "Rust is installed now. Great!" 

  * now open `cmd.exe` and run `pip install countess`.

  * this may take quite some time to install depending on
    which packages are being built from source.
   
# NEXT

Now CountESS is installed, see [Running CountESS](../running-countess/)
