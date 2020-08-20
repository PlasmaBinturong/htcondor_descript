from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()

setup(name='fluidcondor',
      version='0.0.0a0',
      description='Helper tools for working with HTCondor computing cluster scheduler',
      long_description=readme(),
      url='https://github.com/GullumLuvl/fluidcondor',
      author='GullumLuvl',
      author_email='gullumluvlcodes'+('@')+'outlook.com',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Natural Language :: English',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.5',
          'Environment :: Console',
          'Intended Audience :: Science/Research',
          'Topic :: Utilities'
      ],
      license="Unlicense",
      keywords='cluster htcondor',
      packages=['fluidcondor'],
      package_dir={'fluidcondor': ''},
      #py_modules=['fluidcondor.condor_descript',
      #            'fluidcondor.submitsplit',
      #            'fluidcondor.condor_checklogs'],
      install_requires=find_packages(),
      entry_points = {
          'console_scripts': ['condor_checklogs=fluidcondor.condor_checklogs:main',
                              'condor_descript=fluidcondor.condor_descript:main',
                              'submitsplit=fluidcondor.submitsplit:main']
          },
      include_package_data=True,
      zip_safe=False)
