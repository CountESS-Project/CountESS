{
  python3Packages,
  fetchFromGitHub,
  cmake,
}: let

  flit_core = with python3Packages;
    buildPythonPackage rec {
      pname = "flit_core";
      version = "3.9.0";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-cq0mYXbEo/z6tfKTDXaJYFmFEkBXDOmphzO2WMt4bro=";
      };
      format = "pyproject";
      nativeBuildInputs = [hatchling];
      pythonImportsCheck = ["flit_core"];
    };

  fqfa = with python3Packages;
    buildPythonPackage rec {
      pname = "fqfa";
      version = "1.3.0";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-eSX8W5boTtf09NL2KLvBe0HMzzUNxhYjIJ2Jjbf2hEM=";
      };
      format = "pyproject";
      nativeBuildInputs = [hatchling];
      pythonImportsCheck = ["fqfa"];
    };

  more_itertools = with python3Packages;
    buildPythonPackage rec {
      pname = "more-itertools";
      version = "9.1.0";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-yrqjQa0DieqDwXqUVmpTrkydBzSYYeyxTcbQNFz5rF0=";
      };
      format = "pyproject";
      nativeBuildInputs = [hatchling flit_core];
      pythonImportsCheck = ["more_itertools"];
    };

  ttkthemes = with python3Packages;
    buildPythonPackage rec {
      pname = "ttkthemes";
      version = "3.2.2";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-AdrtAB8v8OTzKDKg2epIF2wMUFIDsDB1a93jvRvLIdI=";
      };
      propagatedBuildInputs = [
        tkinter
        pillow
      ];
    };

  blosc2 = with python3Packages;
    buildPythonPackage rec {
      pname = "blosc2";
      version = "2.0.0";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-8ZsLNnT2yCW0kPANgmSwxUDCzcEex+gReNOLg8V3kKE=";
      };
      nativeBuildInputs = [scikit-build cmake cython];
      propagatedBuildInputs = [
        numpy
        msgpack
      ];
      dontUseCmakeConfigure = true;
      format = "pyproject";
    };

  meson = with python3Packages;
    buildPythonPackage rec {
      pname = "meson";
      version = "1.2.3";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-RTOkPDRUjt0fY6J2pCaQ/OFb3pQJvPIMS4+j1+TXysE=";
      };
      nativeBuildInputs = [cython setuptools];
      format = "pyproject";
    };

  packaging = with python3Packages;
    buildPythonPackage rec {
      pname = "packaging";
      version = "23.1";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-o5KYDSts/6ZEQxiYvlSwBFFRMZ0efsNPDP7Uh2fdM08=";
      };
      nativeBuildInputs = [cython setuptools flit_core];
      format = "pyproject";
    };

  pyproject_metadata = with python3Packages;
    buildPythonPackage rec {
      pname = "pyproject-metadata";
      version = "0.7.1";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-CpTxixCLmyHzomo9VB8FbDTtyxfchyoUShVhj+1672c=";
      };
      nativeBuildInputs = [cython setuptools packaging];
      format = "pyproject";
    };

  ninja = with python3Packages;
    buildPythonPackage rec {
      pname = "ninja";
      version = "1.11.1";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-yDOkfTmy0e7j+cqIb6FYHv1b5gaLgnNKwimWHuh0j5A=";
      };
      nativeBuildInputs = [cython cmake setuptools scikit-build tomli];
      dontUseCmakeConfigure = true;
      format = "pyproject";
    };

  meson_python = with python3Packages;
    buildPythonPackage rec {
      pname = "meson_python";
      version = "0.14.0";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-uWhmaQMmVE3+RSWDdTrD9DMTIn6f2UFnAajfkK8hIjQ=";
      };
      nativeBuildInputs = [cython meson setuptools tomli packaging pyproject_metadata ninja];
      format = "pyproject";
    };

  versioneer = with python3Packages;
    buildPythonPackage rec {
      pname = "versioneer";
      version = "0.29";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-WrKDuYVyEdYbUzGLfHks9o55jnZe4Xwnren2ySQjVzE=";
      };
      nativeBuildInputs = [cython setuptools tomli];
      format = "pyproject";
    };

  pandas = with python3Packages;
    buildPythonPackage rec {
      pname = "pandas";
      version = "2.1.0";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-YsJMf8WeQrd1zgZ5z6exSl+b+3ZDz75wjJYGmeBfuRg=";
      };
      nativeBuildInputs = [meson meson_python cython tomli packaging pyproject_metadata ninja setuptools versioneer numpy];
      format = "pyproject";
    };

  rapidfuzz = with python3Packages;
    buildPythonPackage rec {
      pname = "rapidfuzz";
      version = "2.15.1";
      src = fetchFromGitHub {
        owner = "maxbachmann";
        repo = "RapidFuzz";
        rev = "v2.15.1";
        sha256 = "sha256-HYHmhlOuQT86pdYviRDMHC48HE+CefCu20a0hkNVy3k=";
      };
      nativeBuildInputs = [ scikit-build cmake cython ];
      dontUseCmakeConfigure = true;
      format = "pyproject";
  };

  

in
  with python3Packages;
    buildPythonApplication {
      pname = "CountESS";
      version = "git";
      src = ./.;
      propagatedBuildInputs = [
        flit
        fqfa
        more_itertools
        numpy
        pandas
        packaging
        tables
        ttkthemes
        rapidfuzz
      ];

      nativeBuildInputs = [cython];

      meta = {
        mainProgram = "countess_gui";
      };
    }
