from setuptools import setup, find_packages

setup(
    name='job_scheduler',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'fastapi>=0.90.0',
        'uvicorn>=0.20.0',
        'matplotlib>=3.5.0',
        'pytest>=7.0.0',
    ],
    entry_points={
        'console_scripts': [
            'job_scheduler=job_scheduler.api.api_service:app',
        ],
    },
)
