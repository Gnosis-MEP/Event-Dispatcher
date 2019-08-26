from setuptools import setup

setup(
    name='event_dispatcher',
    version='0.1',
    description='Service responsible for reading the buffers from the pre-processor and send them to the correct content extraction based on the Control Flow received by the Query Planner.',
    author='Felipe Arruda Pontes',
    author_email='felipe.arruda.pontes@insight-centre.org',
    packages=['event_dispatcher'],
    zip_safe=False
)
