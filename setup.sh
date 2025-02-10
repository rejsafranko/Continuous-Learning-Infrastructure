#!/bin/bash

ENV_DIR="env"

# Step 1: Run the Python version check.
echo "Checking Python version..."
python -c "from test_environment import check_python_version; check_python_version()"
if [[ $? -ne 0 ]]; then
    echo ">>> Python version check failed!"
    exit 1
fi

# Step 2: Create a virtual environment if it doesn't already exist.
if [[ ! -d "$ENV_DIR" ]]; then
    echo "Creating virtual environment..."
    python3 -m venv "$ENV_DIR"
fi

# Step 3: Activate the virtual environment and install requirements.
echo "Activating virtual environment and installing packages..."
source "$ENV_DIR/bin/activate"
python3 -m pip install -r requirements.txt

# Step 4: Run the package check.
echo "Checking installed packages..."
python -c "from test_environment import check_requirements; check_requirements()"
if [[ $? -ne 0 ]]; then
    echo "Package check failed!"
    exit 1
fi

echo ">>> All tests passed!"