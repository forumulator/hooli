# Contains code for logging
import logging

from inspect import getframeinfo, stack

def initialize():
  caller = getframeinfo(stack()[1][0])
  log_file_name = caller.filename
  print("Configuring logging for %s" %log_file_name)
  log_file_name = log_file_name.strip(".py") + ".log"
  FORMAT = "%(asctime)s \n%(filename)s:%(lineno)s  %(message)s \n"
  logging.basicConfig(format=FORMAT,
    filename=log_file_name, level=logging.INFO)
