# Installing CountESS

## ... using pip

CountESS can be installed from [pypi](https://pypi.org/):

    pip install countess

... or install the latest development version directly from [github](https://github.com/):

    pip install git+https://github.com/CountESS-Project/CountESS.git

## ... using git, for development

Clone the repository and [install for development](https://pip.pypa.io/en/stable/cli/pip_install/#cmdoption-e):

    git clone https://github.com/CountESS-Project/CountESS.git
    cd CountESS
    pip install -e .[dev]

See [Contributing to CountESS](../contributing/) for more information on development.

## ... using nix

CountESS can be installed and run in one command with
[nix](https://nixos.org/):

    nix run github:CountESS-Project/CountESS

## ... under homebrew

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

2. Use homebrew to install a recent Tk library and Python 3.10 into your `$HOME/homebrew/bin`
   directory.  This step is pretty slow:

        brew install tcl-tk python@3.10 python-tk@3.10

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

# NEXT

Now CountESS is installed, see [Running CountESS](../running-countess/)
