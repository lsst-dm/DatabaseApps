import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*") 

# The main call
setup(name='DatabaseApss',
      version ='2.0.2',
      license = "GPL",
      description = "Provide DES database access methods",
      author = "Michelle Gower",
      author_email = "gower@illinois.edu",
      packages = ['databaseapps'],
      package_dir = {'': 'python'},
      scripts = bin_files,
      data_files=[('ups',['ups/DatabaseApps.table'])],
      )

