import setuptools

setuptools.setup(
    name="pywebsocket-rpc",
    version="0.0.4",
    author="jafreck",
    author_email="jafreck@microosft.com",
    description="RPC over websockets",
    long_description_content_type="text/markdown",
    url="https://github.com/jafreck/websocket-rpc",
    packages=setuptools.find_namespace_packages(),
    install_requires=[
        "aiohttp",
        "aiohttp[speedups]",
        "protobuf",
        "pyopenssl",
        "aiojobs",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
