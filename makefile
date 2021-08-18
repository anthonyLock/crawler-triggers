test: 
	pytest

coverage: 
	coverage run --source=. -m pytest && coverage html 