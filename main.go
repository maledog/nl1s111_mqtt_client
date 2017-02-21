// nl1s111_mqtt_client project main.go
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kardianos/osext"
	"github.com/maledog/logrot"
	"github.com/yosssi/gmq/mqtt"
	"github.com/yosssi/gmq/mqtt/client"
)

var mqtt_connected bool
var snrs = []string{"Temperature", "Pressure", "Humidity"}

type Config struct {
	Log          Log          `json:"log"`
	Debug_mqtt   bool         `json:"debug_mqtt"`
	MQTT_options MQTT_options `json:"mqtt_options"`
	Ports        []Port       `json:"ports"`
}

type Log struct {
	File      string `json:"file"`
	Max_size  int64  `json:"max_size"`
	Max_files int    `json:"max_files"`
}

type MQTT_options struct {
	Proto       string        `json:"proto"`
	Host        string        `json:"host"`
	User        string        `json:"user"`
	Password    string        `json:"password"`
	Client_id   string        `json:"client_id"`
	Reconnect_f string        `json:"reconnect"`
	Reconnect   time.Duration `json:"-"`
}

type Port struct {
	Debug            bool          `json:"debug"`
	Is_Enabled       bool          `json:"enabled"`
	Dial             string        `json:"dial"`
	Tcp_io_timeout_f string        `json:"tcp_io_timeout"`
	Tcp_io_timeout   time.Duration `json:"-"`
	Pool_interval_f  string        `json:"poll_interval"`
	Pool_interval    time.Duration `json:"-"`
	Devices          []Device      `json:"devices"`
}

type Device struct {
	Is_Enabled bool   `json:"enabled"`
	Address    int    `json:"address"`
	Type       string `json:"type"`
	Name       string `json:"name"`
	Topic      string `json:"topic"`
}

type MQTT_data struct {
	Topic string
	Value string
	Time  time.Time
}

type Resp struct {
	Value string
	Err   string
}

func main() {
	exe_dir, err := osext.ExecutableFolder()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	var in_config string
	var mc bool
	var dm bool
	var ds bool
	flag.StringVar(&in_config, "c", exe_dir+"/config.json", "config file")
	flag.BoolVar(&mc, "m", false, "make config file")
	flag.BoolVar(&dm, "dm", false, "debug mqtt stdout")
	flag.BoolVar(&ds, "ds", false, "debug serial stdout")
	flag.Parse()
	if mc {
		cf, err := make_config(in_config)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		} else {
			log.Printf("Config file:%s\n", cf)
			os.Exit(0)
		}
	}
	cnf, err := get_config(in_config)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	if dm || ds {
		cnf.Log.File = ""
		if dm {
			cnf.Debug_mqtt = true
		}
		if ds {
			for i := 0; i < len(cnf.Ports); i++ {
				cnf.Ports[i].Debug = true
			}
		}
	}
	if cnf.Log.File != "" {
		f, err := logrot.Open(cnf.Log.File, 0644, cnf.Log.Max_size, cnf.Log.Max_files)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		defer f.Close()
		log.SetOutput(f)
	}
	err = cnf.Parse_time()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	ch_os_signals := make(chan os.Signal, 1)
	ch_mqtt_close := make(chan struct{}, 1)
	signal.Notify(ch_os_signals, os.Interrupt, os.Kill, syscall.SIGTERM)
	ch_output := make(chan MQTT_data, 512)
	chs_logger_close := make(map[string]chan struct{})
	go mqtt_client_wrapper(cnf, ch_output, ch_mqtt_close)
	for _, port := range cnf.Ports {
		if port.Is_Enabled {
			ch_logger_close := make(chan struct{}, 1)
			chs_logger_close[port.Dial] = ch_logger_close
			go logger(port, ch_output, ch_logger_close)
		}
	}
	<-ch_os_signals
	log.Println("Interrupted program.")
	ch_mqtt_close <- struct{}{}
	for _, port := range cnf.Ports {
		if port.Is_Enabled {
			chs_logger_close[port.Dial] <- struct{}{}
		}
	}
}

func mqtt_client_wrapper(cnf Config, ch_output chan MQTT_data, ch_mqtt_close chan struct{}) {
	ch_mqtt_client_close := make(chan struct{}, 1)
	for {
		select {
		case <-ch_mqtt_close:
			ch_mqtt_client_close <- struct{}{}
			break
		default:
			{
				err := mqtt_client(cnf, ch_output, ch_mqtt_client_close)
				if err != nil {
					log.Printf("MQTT:ERROR:%v.\n", err)
				}
				mqtt_connected = false
				log.Printf("MQTT:Next connect in %s.\n", cnf.MQTT_options.Reconnect.String())
				time.Sleep(cnf.MQTT_options.Reconnect)
			}
		}
	}
}

func mqtt_client(cnf Config, ch_output chan MQTT_data, ch_mqtt_client_close chan struct{}) error {
	ch_mqtt_error := make(chan error, 1)
	var opts client.Options
	opts.ErrorHandler = func(err error) {
		ch_mqtt_error <- err
	}
	var conn_opts client.ConnectOptions
	conn_opts.Network = cnf.MQTT_options.Proto
	conn_opts.Address = cnf.MQTT_options.Host
	conn_opts.ClientID = []byte(cnf.MQTT_options.Client_id)
	conn_opts.CleanSession = true
	conn_opts.UserName = []byte(cnf.MQTT_options.User)
	conn_opts.Password = []byte(cnf.MQTT_options.Password)
	conn_opts.KeepAlive = 30
	cli := client.New(&opts)
	err := cli.Connect(&conn_opts)
	if err != nil {
		return err
	}
	mqtt_connected = true
	log.Printf("MQTT:Connected to %s.\n", cnf.MQTT_options.Host)
	defer cli.Terminate()
	go make_mqtt_skel(cnf, ch_output)
	for {
		select {
		case <-ch_mqtt_client_close:
			err := cli.Disconnect()
			log.Printf("MQTT:Client disconnected.\n")
			return err
		case err := <-ch_mqtt_error:
			return err
		case data := <-ch_output:
			{
				if cnf.Debug_mqtt {
					log.Printf("%s << %s", data.Topic, data.Value)
				}
				err = cli.Publish(&client.PublishOptions{
					QoS:       mqtt.QoS0,
					TopicName: []byte(data.Topic),
					Message:   []byte(data.Value),
				})
				if err != nil {
					return err
				}
			}
		}
	}
}

func make_mqtt_skel(cnf Config, ch_output chan MQTT_data) {
	var out_data []MQTT_data
	var data MQTT_data
	for _, port := range cnf.Ports {
		if port.Is_Enabled {
			for _, device := range port.Devices {
				if device.Is_Enabled {
					var ni int
					switch device.Type {
					case "nl3dpas":
						{
							ni = 3
						}
					case "nl1s111":
						{
							ni = 1
						}
					default:
						{
							log.Printf("Unknown type: %s", device.Type)
							continue
						}
					}
					data.Time = time.Now()
					data.Topic = device.Topic + "/meta/name"
					data.Value = device.Name
					out_data = append(out_data, data)
					for i := 0; i < ni; i++ {
						data.Topic = fmt.Sprintf("%s/controls/%s/meta/type", device.Topic, snrs[i])
						data.Value = "value"
						out_data = append(out_data, data)
						data.Topic = fmt.Sprintf("%s/controls/%s/meta/readonly", device.Topic, snrs[i])
						data.Value = "1"
						out_data = append(out_data, data)
						data.Topic = fmt.Sprintf("%s/controls/%s/meta/order", device.Topic, snrs[i])
						data.Value = fmt.Sprintf("%d", i+1)
						out_data = append(out_data, data)
					}
				}
			}
		}
	}
	for _, data := range out_data {
		select {
		case ch_output <- data:
		default:
		}
	}
}

func logger(port Port, ch_output chan MQTT_data, ch_logger_close chan struct{}) {
	ticker := time.NewTicker(port.Pool_interval)
	for {
		select {
		case <-ch_logger_close:
			{
				ticker.Stop()
				break
			}
		case <-ticker.C:
			{
				if mqtt_connected {
					var out_data []MQTT_data
					for _, device := range port.Devices {
						if device.Is_Enabled {
							var data MQTT_data
							resps, err := port_read_data(port, device)
							if err != nil {
								log.Println(err)
							}
							for i, resp := range resps {
								data.Time = time.Now()
								data.Topic = fmt.Sprintf("%s/controls/%s", device.Topic, snrs[i])
								if resp.Err == "" {
									data.Value = resp.Value
									out_data = append(out_data, data)
								}
								data.Topic = data.Topic + "/meta/error"
								data.Value = resp.Err
								out_data = append(out_data, data)
							}
						}
					}
					for _, data := range out_data {
						select {
						case ch_output <- data:
						case <-time.After(time.Second * 5):
							{
								log.Println("write pub operation timeout")
							}
						}
					}
				}
			}
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func port_read_data(port Port, device Device) (resps []Resp, err error) {
	var send_packet []byte
	var resps_e []Resp
	switch device.Type {
	case "nl3dpas":
		{
			send_packet = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x06, byte(device.Address), byte(4), 0x00, 0x00, 3 >> 8, 3 & 0xff}
			resps_e = device_error(3)
		}
	case "nl1s111":
		{
			send_packet = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x06, byte(device.Address), byte(4), 0x00, 0x00, 1 >> 8, 1 & 0xff}
			resps_e = device_error(1)
		}
	default:
		{
			err = errors.New(fmt.Sprintf("Unknown type: %s", device.Type))
			return
		}
	}
	conn, err := net.DialTimeout("tcp4", port.Dial, port.Tcp_io_timeout)
	if err != nil {
		resps = resps_e
		return
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	conn.SetDeadline(time.Now().Add(port.Tcp_io_timeout))
	_, err = writer.Write(send_packet)
	if err != nil {
		resps = resps_e
		return
	}
	err = writer.Flush()
	if err != nil {
		resps = resps_e
		return
	}
	if port.Debug {
		log.Printf("% x >> %s\n", send_packet, port.Dial)
	}
	buf := make([]byte, 512)
	n, err := reader.Read(buf)
	if err != nil {
		resps = resps_e
		return
	}
	recv_packet := buf[:n]
	if port.Debug {
		log.Printf("% x << %s\n", recv_packet, port.Dial)
	}
	err = mbtcp_check_packet_len(recv_packet)
	if err != nil {
		resps = resps_e
		return
	}
	err = modbus_check_error(recv_packet[6:], send_packet[6:])
	if err != nil {
		resps = resps_e
		return
	}
	ld := int(recv_packet[8])
	data := recv_packet[9:]
	if ld != 6 && ld != 2 || ld != len(data) {
		err = errors.New(fmt.Sprintf("Error length data: %d:%d", ld, len(data)))
		resps = resps_e
		return
	}
	var resp Resp
	for i := 0; i < ld/2; i++ {
		var ti int16
		b := bytes.NewReader(data[i*2 : (i+1)*2])
		err = binary.Read(b, binary.BigEndian, &ti)
		if err != nil {
			resp.Err = "r"
		} else {
			resp.Value = fmt.Sprintf("%f", float32(ti)*0.1)
			resp.Err = ""
		}
		if port.Debug {
			log.Printf("%d: %s: %s, %v\n", device.Address, snrs[i], resp.Value, err)
		}
		resps = append(resps, resp)
	}
	return
}

func device_error(n int) (resps []Resp) {
	for i := 0; i < n; i++ {
		resps = append(resps, Resp{"", "r"})
	}
	return
}

func mbtcp_check_packet_len(adu []byte) error {
	if len(adu) > 260 || len(adu) < 6 {
		return errors.New("ADU Modbus TCP length must be between 6 and 260 bytes.")
	}
	apdu := adu[6:]
	len_apdu := binary.BigEndian.Uint16(adu[4:6])
	if int(len_apdu) != len(apdu) {
		return errors.New("PDU field does not match length of the PDU.")
	}
	return nil
}

func modbus_check_error(rcv []byte, snd []byte) error {
	if snd[0] != rcv[0] {
		return errors.New("slave addresses do not match")
	}
	if rcv[1] != snd[1] {
		if rcv[1]&0x7f == snd[1] {
			switch rcv[2] {
			case 1:
				return errors.New("modbus illegal function")
			case 2:
				return errors.New("modbus illegal data address")
			case 3:
				return errors.New("modbus illegal data value")
			case 4:
				return errors.New("modbus slave device failure")
			default:
				return errors.New("modbus unspecified error")
			}
		} else {
			return errors.New("modbus error")
		}
	}
	return nil
}

func (c *Config) Parse_time() error {
	interval, err := time.ParseDuration(c.MQTT_options.Reconnect_f)
	if err != nil {
		return err
	}
	c.MQTT_options.Reconnect = interval
	for i := 0; i < len(c.Ports); i++ {
		interval, err := time.ParseDuration(c.Ports[i].Tcp_io_timeout_f)
		if err != nil {
			return err
		}
		c.Ports[i].Tcp_io_timeout = interval
		interval, err = time.ParseDuration(c.Ports[i].Pool_interval_f)
		if err != nil {
			return err
		}
		c.Ports[i].Pool_interval = interval
	}
	return nil
}

func get_config(cf string) (cnf Config, err error) {
	data, err := ioutil.ReadFile(cf)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &cnf)
	return
}

func make_config(cf string) (string, error) {
	var cnf Config
	cnf.Log.File = "logs/nl1s111_mqtt_client.log"
	cnf.Log.Max_size = 10 * 1024 * 1024
	cnf.Log.Max_files = 10
	cnf.Debug_mqtt = false
	cnf.MQTT_options.Proto = "tcp"
	cnf.MQTT_options.Host = "localhost:1883"
	cnf.MQTT_options.User = ""
	cnf.MQTT_options.Password = ""
	cnf.MQTT_options.Client_id = "nl1s111_mqtt_client"
	cnf.MQTT_options.Reconnect_f = "60s"
	var port Port
	var device Device
	port.Debug = false
	port.Is_Enabled = true
	port.Dial = "127.0.0.1:15501"
	port.Tcp_io_timeout_f = "5s"
	port.Pool_interval_f = "15s"
	device.Is_Enabled = true
	device.Address = 1
	device.Type = "nl1s111"
	device.Name = "nl-1s111-1"
	device.Topic = "/devices/nl-1s111-1"
	port.Devices = append(port.Devices, device)
	device.Is_Enabled = true
	device.Address = 2
	device.Type = "nl3dpas"
	device.Name = "nl-3dpas-1"
	device.Topic = "/devices/nl-3dpas-1"
	port.Devices = append(port.Devices, device)
	cnf.Ports = append(cnf.Ports, port)
	port.Debug = false
	port.Is_Enabled = true
	port.Dial = "127.0.0.1:15504"
	port.Tcp_io_timeout_f = "5s"
	port.Pool_interval_f = "15s"
	device.Is_Enabled = true
	device.Address = 3
	device.Type = "nl1s111"
	device.Name = "nl-1s111-2"
	device.Topic = "/devices/nl-1s111-2"
	port.Devices = []Device{}
	port.Devices = append(port.Devices, device)
	device.Is_Enabled = true
	device.Address = 4
	device.Type = "nl3dpas"
	device.Name = "nl-3dpas-2"
	device.Topic = "/devices/nl-3dpas-2"
	port.Devices = append(port.Devices, device)
	cnf.Ports = append(cnf.Ports, port)
	data, err := json.MarshalIndent(cnf, "", "\t")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(cf, data, 0644)
	if err != nil {
		return "", err
	}
	return cf, nil
}
