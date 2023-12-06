run:
	mkdir -p export/temp/English &&\
	mkdir -p export/temp/Spanish &&\
	python2 adc_export.py ${config_file} 645  --debug
	python2 adc_export.py ${config_file} 9493 --debug
