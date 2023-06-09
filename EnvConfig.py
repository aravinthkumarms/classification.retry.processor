from dotenv import dotenv_values
config = dotenv_values(".env")
PYRODBUserName = config['PYROUSERNAME']
PYRODBPassword = config['PYROPASSWORD']
PYRODBPort = config['PYROPORT']