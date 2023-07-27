from distutils.core import setup

requirements = [
    # Use ./environment.yml for deps.
]

setup(
    name='xcube-geodb-places',
    version='1.0',
    packages=['xcube_places_plugin', 'xcube_places_plugin.api',
              'xcube_places_plugin.server'],
    url='https://github.com/dcs4cop/xcube-geodb-places',
    license='MIT License',
    author='thomasstorm',
    description='A plugin for xcube-server that adds places loaded from the xcube geoDB.',
    install_requires=requirements
)
