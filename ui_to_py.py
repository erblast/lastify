# Usage of Qt Designer
# compile .py out of .ui file using python console/skript

import PyQt4.uic
with open('gui.py','w') as f:
	PyQt4.uic.compileUi('gui.ui',f)
