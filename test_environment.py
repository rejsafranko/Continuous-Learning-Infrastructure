import os
import sys
import pkg_resources


REQUIRED_PYTHON = "python3.11.9"
REQUIREMENTS_FILE = "requirements.txt"


def check_python_version():
    system_version = sys.version.replace("\n", "")

    if REQUIRED_PYTHON.startswith("python"):
        required_version_str = REQUIRED_PYTHON[len("python") :].strip()
        required_version_parts = required_version_str.split(".")
        if len(required_version_parts) != 3:
            raise ValueError(
                "Invalid required Python version format: {}".format(REQUIRED_PYTHON)
            )

        required_major = int(required_version_parts[0])
        required_minor = int(required_version_parts[1])
        required_micro = int(required_version_parts[2])
    else:
        raise ValueError("Unrecognized python interpreter: {}".format(REQUIRED_PYTHON))

    system_major = sys.version_info.major
    system_minor = sys.version_info.minor
    system_micro = sys.version_info.micro

    if (system_major, system_minor, system_micro) != (
        required_major,
        required_minor,
        required_micro,
    ):
        raise TypeError(
            "This project requires Python {}.{}.{}. Found: Python {}".format(
                required_major, required_minor, required_micro, system_version
            )
        )

    print("\n>>> Python version test passed!\n")


def check_requirements():
    if not os.path.exists(REQUIREMENTS_FILE):
        raise FileNotFoundError(f"{REQUIREMENTS_FILE} not found.")

    with open(REQUIREMENTS_FILE, "r") as f:
        for line in f:
            package_spec = line.strip()
            if package_spec:
                try:
                    pkg_resources.require(package_spec)
                except pkg_resources.DistributionNotFound:
                    raise ImportError(
                        f"Required package {package_spec} not found. "
                        f"Please install it by running `pip install -r {REQUIREMENTS_FILE}`."
                    )
                except pkg_resources.VersionConflict as e:
                    raise ImportError(f"Version conflict for {package_spec}: {e}.")

    print("\n>>> Package installation verified!\n")
