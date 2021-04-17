# -*- coding: utf-8 -*-
import sys

required_verion = (3,6)
if sys.version_info < required_verion:
    raise ValueError('needs at least python {}! You are trying to install it under python {}'.format('.'.join(str(i) for i in required_verion), sys.version))

# import ez_setup
# ez_setup.use_setuptools()

from setuptools import setup
# from distutils.core import setup
setup(
    name="nesdis_aws",
    version="0.1",
    packages=['nesdis_aws'],
    author="Hagen Telg",
    author_email="hagen@hagnet.net",
    description="A library with the goal to simplify downloading NOAA-NESDIS satellite data from the Amazon Web Services.",
    license="MIT",
    keywords="nesdis_aws",
    url="https://github.com/hagne/nesdis_aws",
    # scripts=['scripts/scrape_sat', 
    #          # 'scripts/hrrr_smoke2gml'
    #          ],
    # install_requires=['numpy','pandas'],
    # extras_require={'plotting': ['matplotlib'],
    #                 'testing': ['scipy']},
    # test_suite='nose.collector',
    # tests_require=['nose'],
)
