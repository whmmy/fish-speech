
cp tts_process_script.sh /etc/init.d/tts_process_script
update-rc.d tts_process_script defaults
update-rc.d -n tts_process_script defaults
service tts_process_script start

chmod +x /etc/init.d/tts_process_script
service tts_process_script stop
service tts_process_script start