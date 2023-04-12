{
  python3Packages,
  fetchFromGitHub,
  cmake,
}: let
  fqfa = with python3Packages;
    buildPythonPackage rec {
      pname = "fqfa";
      version = "1.2.3";
      src = fetchPypi {
        inherit pname version;
        sha256 = "sha256-ex0IJfbFWTdFQhXWxtUeH5OQPvaU7LoRnKL9QWyKBos=";
      };
      format = "pyproject";
      nativeBuildInputs = [hatchling];
      pythonImportsCheck = ["fqfa"];
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
        dask
        distributed
        fqfa
        more-itertools
        numpy
        pandas
        tables
        ttkthemes
        rapidfuzz
      ];

      nativeBuildInputs = [cython];

      meta = {
        mainProgram = "countess_gui";
      };
    }
