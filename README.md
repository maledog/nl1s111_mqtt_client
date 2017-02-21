nl1s111_mqtt_client
===========
It reads data from the sensors [NL-1S111 and NL-3DPAS](http://www.reallab.ru/Data%20Conditioning%20Circuits.htm) and publishes them in MQTT. It works in conjunction with [ssc3_serial_proxy](https://github.com/maledog/ssc3_serial_proxy). Use `nl1s111_mqtt_client -h` to get help.

**Dependencies**
```
github.com/kardianos/osext
github.com/maledog/logrot
github.com/yosssi/gmq/mqtt
github.com/yosssi/gmq/mqtt/client
```
