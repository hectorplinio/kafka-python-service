# Python Server Base Template

## Description

This repository serves as a template for quickly setting up Python servers using Flask. It includes basic setup and configurations to help you start building your application swiftly.

## Project Structure

The project is organized into the following parts:

- `src`: Contains the source code for the server.

## Table of Contents

- [Python Server Base Template](#python-server-base-template)
  - [Description](#description)
  - [Project Structure](#project-structure)
  - [Environment Setup](#environment-setup)
    - [Requirements](#requirements)
    - [Installation](#installation)
    - [Using the Template](#using-the-template)
  - [Running the Application](#running-the-application)
    - [Docker](#docker)
  - [Folder Structure](#folder-structure)
  - [Development](#development)
    - [Style Guide](#style-guide)
    - [Testing](#testing)
      - [Running Tests](#running-tests)
  - [Contributing](#contributing)
  - [License](#license)

## Environment Setup

### Requirements

- Python >= 3.8
- pip >= 20.0
- Docker (optional, for containerization)

### Installation

1. Clone the repository:

```bash
git clone https://github.com/hectorplinio/python-server-base.git
cd python-server-base
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

3. Install the dependencies:

```bash
pip install -r requirements.txt
```

### Using the Template

To create a new repository using this template:

1. Go to the [template repository](https://github.com/hectorplinio/python-server-base).
2. Click on "Use this template".
3. Create your new repository.

## Running the Application

To run the application we are going to use Docker.

### Docker

Use the following commands:

1. Build the Docker image:

```bash
 docker build -t python-server-base .
```

2. Run the Docker container:

```bash
 docker run -p 3010:3010 python-server-base
```

## Folder Structure

```plaintext
server-base-python/
├── src/ # Source code
│ ├── app.py # Initialize the app
│ ├── __init__.py # Package marker
│ ├── config.py # Configuration settings
│ ├── controllers/ # Controllers for handling requests
│ │ ├── __init__.py
│ │ └── health_controller.py
│ ├── services/ # Services for business logic
│ │ ├── __init__.py
│ │ └── health_service.py
├── tests/ # Unit tests
│ ├── __init__.py
│ ├── test_health_service.py
│ └── test_health_controller.py
├── .github/ # GitHub configuration files
├── Dockerfile # Dockerfile for containerization
├── requirements.txt # Project dependencies
├── Makefile # Makefile for automation
└── README.md # Project documentation
```

## Development

### Style Guide

Before submitting a patch, please make sure that the code is formatted by executing this command:

```bash
make format
```

### Testing

Testing is crucial for us and we strive for high coverage of our code.

We encourage you to write tests for every functionality you build and also update the existing ones if they need to.

#### Running Tests

Before running the tests, install the needed dependencies:

```bash
pip install -r requirements.txt
```

Execute all tests with:

```bash
pytest
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you have any improvements or bug fixes.

## License

This project is licensed under the MIT License.