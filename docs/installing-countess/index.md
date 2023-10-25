# Installing CountESS

## ... using pip

CountESS can be installed from [pypi](https://pypi.org/):

    pip install countess

... or install the latest development version directly from [github](https://github.com/):

    pip install git+https://github.com/CountESS-Project/CountESS.git

... or download the source and [install for development](https://pip.pypa.io/en/stable/cli/pip_install/#cmdoption-e):

    git clone https://github.com/CountESS-Project/CountESS.git
    cd CountESS
    pip install -e .

## ... using nix

CountESS can be installed and run with nix:

    nix run github:CountESS-Project/CountESS

## ... under homebrew


Even recent Mac installs use a very old version of Python and Tk so
we use [homebrew](https://brew.sh/) to install a recent version.

1. download the homebrew tarball & make its binaries available from your shell.

   These instructions are for installing on a Mac without needing Administrator access.
   If you already have homebrew installed you can skip this step:

        mkdir bin homebrew
        curl -L https://github.com/Homebrew/brew/tarball/master | tar xz --strip 1 -C homebrew
        echo 'export PATH=$HOME/bin:$HOME/homebrew/bin:$PATH' >> .bash_profile
        source .bash_profile

2. Use homebrew to install a recent Tk and Python 3.10 into your `$HOME/homebrew/bin`
   directory.  This step is pretty slow as it has to compile stuff:

        brew install tcl-tk python@3.10 python-tk@3.10
        ln -s $HOME/homebrew/bin/python3.10 bin/python
        ln -s $HOME/homebrew/bin/pip3.10 bin/pip

3. Install countess using pip:

        pip install countess
