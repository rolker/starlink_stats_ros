from setuptools import find_packages, setup

package_name = 'starlink_stats'

setup(
    name=package_name,
    version='0.1.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/launch', ['launch/starlink_stats.launch.py']),
    ],
    install_requires=['setuptools'],
    tests_require=['pytest'],
    zip_safe=True,
    maintainer='Avery Munoz',
    maintainer_email='avery.munoz@unh.edu',
    description='Starlink dish diagnostics via gRPC, published as ROS 2 DiagnosticArray',
    license='BSD-3-Clause',
    entry_points={
        'console_scripts': [
            'starlink_diagnostics_node = starlink_stats.starlink_diagnostics_node:main',
        ],
    },
)
