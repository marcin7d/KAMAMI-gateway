#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cJSON.h"
#include "driver/gpio.h"
#include "esp_eth.h"
#include "esp_event.h"
#include "esp_now.h"
#include "esp_http_server.h"
#include "esp_log.h"
#include "esp_mac.h"
#include "esp_netif.h"
#include "esp_rom_sys.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "lwip/netdb.h"
#include "lwip/sockets.h"
#include "mqtt_client.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "mosq_broker.h"

#define FW_NAME "KAMAMI Universal Gateway"
#define FW_VERSION "0.2.0-idf"
#define MAX_GPIO_RULES 12
#define MAX_MODBUS_RULES 12
#define MAX_SENSOR_RULES 12
#define MAX_MQTT_LOG 48
#define MQTT_LOG_PAYLOAD_LEN 192
#define MQTT_LOCAL_CLIENT_ID "kamami-gateway-local"
#define CONFIG_NAMESPACE "gw"
#define CONFIG_KEY "config"

#define ETH_PHY_ADDR 0
#define ETH_PHY_RST_GPIO -1
#define ETH_MDC_GPIO 23
#define ETH_MDIO_GPIO 18
#define ETH_RMII_CLK_GPIO 0

typedef enum { NET_ETH = 0, NET_WIFI_STA = 1, NET_WIFI_AP = 2, NET_ETH_WIFI_AP = 3 } net_mode_t;
typedef enum { MQTT_OFF = 0, MQTT_CLIENT = 1, MQTT_BROKER = 2 } mqtt_mode_t;
typedef enum { MB_TOPIC_TO_REG = 0, MB_REG_TO_TOPIC = 1, MB_BOTH = 2 } modbus_dir_t;
typedef enum { SENSOR_DS18B20 = 0, SENSOR_BME280 = 1, SENSOR_SHT3X = 2, SENSOR_DHT22 = 3, SENSOR_DHT11 = 4 } sensor_type_t;
typedef enum { TEMP_C = 0, TEMP_F = 1, TEMP_K = 2 } temp_unit_t;
typedef enum { PRESS_HPA = 0, PRESS_PA = 1, PRESS_KPA = 2, PRESS_BAR = 3 } press_unit_t;

typedef struct {
    bool enabled;
    int pin;
    bool inverted;
    bool retain;
    char name[24];
    char topic[96];
} gpio_rule_t;

typedef struct {
    bool enabled;
    modbus_dir_t direction;
    char name[24];
    char topic[96];
    char host[64];
    int port;
    int unit;
    int reg;
    int poll_ms;
    int last_value;
    bool has_value;
    uint32_t next_poll_ms;
} modbus_rule_t;

typedef struct {
    bool enabled;
    sensor_type_t type;
    char name[24];
    char topic[96];
    char rom[17];
    int pin;
    int sda;
    int scl;
    int addr;
    int poll_ms;
    int resolution;
    temp_unit_t temp_unit;
    press_unit_t press_unit;
    float temp_offset;
    float hum_offset;
    float press_offset;
    float last_temp;
    float last_hum;
    float last_press;
    bool has_temp;
    bool has_hum;
    bool has_press;
    uint32_t next_poll_ms;
} sensor_rule_t;

typedef struct {
    char host[32];
    int ui_lang;
    net_mode_t net_mode;
    bool dhcp;
    char ip[16];
    char gateway[16];
    char subnet[16];
    char dns[16];
    char sta_ssid[33];
    char sta_pass[65];
    char ap_ssid[33];
    char ap_pass[65];
    bool espnow_enabled;
    int espnow_channel;
    char espnow_peer[18];
    char espnow_rx_topic[96];
    char espnow_tx_topic[96];
    mqtt_mode_t mqtt_mode;
    char mqtt_host[64];
    int mqtt_port;
    char mqtt_user[40];
    char mqtt_pass[64];
    char base_topic[64];
    int broker_port;
    char broker_user[40];
    char broker_pass[64];
    gpio_rule_t rules[MAX_GPIO_RULES];
    modbus_rule_t modbus[MAX_MODBUS_RULES];
    sensor_rule_t sensors[MAX_SENSOR_RULES];
} gateway_config_t;

typedef struct {
    uint32_t seq;
    uint32_t at_ms;
    int len;
    int qos;
    int retain;
    char client[40];
    char topic[96];
    char payload[MQTT_LOG_PAYLOAD_LEN];
} mqtt_log_entry_t;

static const char *TAG = "kamami_gateway";
static gateway_config_t g_cfg;
static httpd_handle_t g_http = NULL;
static esp_mqtt_client_handle_t g_mqtt = NULL;
static bool g_mqtt_online = false;
static bool g_mqtt_local = false;
static bool g_eth_online = false;
static bool g_wifi_online = false;
static bool g_wifi_should_connect = false;
static bool g_espnow_ready = false;
static bool g_network_restart_required = false;
static char g_active_ip[16] = "0.0.0.0";
static int64_t g_boot_us = 0;
static TaskHandle_t g_broker_task = NULL;
static bool g_broker_running = false;
static TaskHandle_t g_modbus_task = NULL;
static TaskHandle_t g_sensor_task = NULL;
static SemaphoreHandle_t g_mqtt_log_lock;
static mqtt_log_entry_t g_mqtt_log[MAX_MQTT_LOG];
static uint32_t g_mqtt_log_seq = 0;
static int g_mqtt_log_next = 0;
static int g_mqtt_log_count = 0;

extern const uint8_t web_index_html_start[] asm("_binary_index_html_start");
extern const uint8_t web_index_html_end[] asm("_binary_index_html_end");

static void start_mqtt_client(void);
static void start_mqtt_local_client(void);
static void broker_task(void *arg);
static void modbus_task(void *arg);
static void sensor_task(void *arg);

static const char index_html[] __attribute__((unused)) =
"<!doctype html><html lang=pl><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>"
"<title>KAMAMI Gateway</title><style>"
":root{color-scheme:dark;--bg:#101317;--p:#1d242b;--l:#34404a;--t:#eef4f8;--m:#9cabb7;--a:#4cc9f0;--b:#ef476f}"
"*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--t);font-family:system-ui,Segoe UI,Arial,sans-serif}"
"header{display:flex;justify-content:space-between;gap:12px;align-items:center;padding:18px 20px;border-bottom:1px solid var(--l)}h1{font-size:20px;margin:0}main{max-width:1180px;margin:auto;padding:18px;display:grid;gap:16px}"
"section{background:var(--p);border:1px solid var(--l);border-radius:8px;padding:16px}h2{font-size:15px;margin:0 0 14px;color:#cbd5df}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}"
"label{display:block}.label{display:block;font-size:12px;color:var(--m);margin-bottom:5px}input,select{width:100%;background:#121820;color:var(--t);border:1px solid var(--l);border-radius:6px;padding:9px}button{border:0;border-radius:6px;background:var(--a);color:#071217;padding:9px 13px;font-weight:700}.secondary{background:#303a43;color:var(--t)}.danger{background:var(--b);color:white}"
".status{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}.tile{border:1px solid var(--l);border-radius:8px;padding:10px}.rule{display:grid;grid-template-columns:80px 90px 1fr 110px 110px 120px;gap:8px;align-items:end;border-top:1px solid var(--l);padding-top:10px;margin-top:10px}.row{display:flex;gap:8px;flex-wrap:wrap}.pill{font-size:12px;color:var(--m);background:#303a43;border-radius:999px;padding:4px 9px}@media(max-width:760px){.rule{grid-template-columns:1fr 1fr}.wide{grid-column:1/-1}}"
"</style></head><body><header><div><h1>KAMAMI Universal Gateway</h1><span class=pill id=fw>...</span></div><button onclick=save()>Zapisz i restartuj</button></header><main>"
"<section><h2>Status</h2><div class=status id=status></div></section>"
"<section><h2>Sieć</h2><div class=grid>"
"<label><span class=label>Host</span><input id=host></label><label><span class=label>Tryb</span><select id=net_mode><option value=0>Ethernet</option><option value=1>WiFi STA</option><option value=2>WiFi AP</option><option value=3>Ethernet + AP</option></select></label><label><span class=label>DHCP</span><select id=dhcp><option value=1>Tak</option><option value=0>Statyczny</option></select></label>"
"<label><span class=label>IP</span><input id=ip></label><label><span class=label>Brama</span><input id=gateway></label><label><span class=label>Maska</span><input id=subnet></label><label><span class=label>DNS</span><input id=dns></label>"
"<label><span class=label>STA SSID</span><input id=sta_ssid></label><label><span class=label>STA hasło</span><input id=sta_pass type=password></label><label><span class=label>AP SSID</span><input id=ap_ssid></label><label><span class=label>AP hasło</span><input id=ap_pass type=password></label>"
"</div></section>"
"<section><h2>MQTT</h2><div class=grid>"
"<label><span class=label>Tryb</span><select id=mqtt_mode><option value=0>Wyłączony</option><option value=1>Klient esp-mqtt</option><option value=2>Broker Espressif Mosquitto</option></select></label><label><span class=label>Host brokera zewn.</span><input id=mqtt_host></label><label><span class=label>Port</span><input id=mqtt_port type=number></label><label><span class=label>Użytkownik klienta</span><input id=mqtt_user></label><label><span class=label>Hasło klienta</span><input id=mqtt_pass type=password></label><label><span class=label>Base topic</span><input id=base_topic></label><label><span class=label>Broker user</span><input id=broker_user></label><label><span class=label>Broker pass</span><input id=broker_pass type=password></label>"
"</div></section><section><h2>GPIO topic map</h2><div id=rules></div><div class=row style='margin-top:12px'><button class=secondary onclick=addRule()>Dodaj wyjście</button><button class=secondary onclick=loadCfg()>Odśwież</button></div></section></main>"
"<script>"
"let cfg={rules:[]};const ids=['host','net_mode','dhcp','ip','gateway','subnet','dns','sta_ssid','sta_pass','ap_ssid','ap_pass','mqtt_mode','mqtt_host','mqtt_port','mqtt_user','mqtt_pass','base_topic','broker_user','broker_pass'];"
"function q(id){return document.getElementById(id)}function set(id,v){q(id).value=(v==null?'':v)}async function loadCfg(){cfg=await(await fetch('/api/config')).json();ids.forEach(id=>set(id,cfg[id]));q('dhcp').value=cfg.dhcp?1:0;renderRules()}"
"function renderRules(){let e=q('rules');e.innerHTML='';(cfg.rules||[]).forEach((r,i)=>{let d=document.createElement('div');d.className='rule';d.innerHTML=`<label><span class=label>Aktywne</span><select data-k=enabled data-i=${i}><option value=1>Tak</option><option value=0>Nie</option></select></label><label><span class=label>GPIO</span><input data-k=pin data-i=${i} type=number></label><label class=wide><span class=label>Topic SET</span><input data-k=topic data-i=${i}></label><label><span class=label>Nazwa</span><input data-k=name data-i=${i}></label><label><span class=label>Odwrócone</span><select data-k=inverted data-i=${i}><option value=0>Nie</option><option value=1>Tak</option></select></label><div class=row><button class=secondary onclick=pulse(${i},1)>ON</button><button class=secondary onclick=pulse(${i},0)>OFF</button><button class=danger onclick=delRule(${i})>Usuń</button></div>`;e.appendChild(d);['enabled','pin','topic','name','inverted'].forEach(k=>{let n=d.querySelector(`[data-k=${k}]`);n.value=(r[k]===true?1:r[k]===false?0:r[k]);n.oninput=()=>{let v=n.value;if(k=='pin')v=+v;if(k=='enabled'||k=='inverted')v=v==='1';cfg.rules[i][k]=v}})})}"
"function addRule(){cfg.rules=cfg.rules||[];cfg.rules.push({enabled:true,pin:2,name:'OUT',topic:(cfg.base_topic||'kamami/gateway')+'/gpio/2/set',inverted:false,retain:true});renderRules()}function delRule(i){cfg.rules.splice(i,1);renderRules()}"
"async function save(){ids.forEach(id=>cfg[id]=q(id).value);cfg.net_mode=+cfg.net_mode;cfg.dhcp=q('dhcp').value==='1';cfg.mqtt_mode=+cfg.mqtt_mode;cfg.mqtt_port=+cfg.mqtt_port;await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});alert('Zapisano, restartuję urządzenie')}"
"async function pulse(i,value){await fetch('/api/gpio',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({index:i,value})})}"
"async function status(){try{let s=await(await fetch('/api/status')).json();q('fw').textContent=s.fw+' '+s.version;q('status').innerHTML=Object.entries(s).map(([k,v])=>`<div class=tile><span class=label>${k}</span><b>${v}</b></div>`).join('')}catch(e){}}loadCfg();status();setInterval(status,2500)"
"</script></body></html>";

static void copy_str(char *dst, size_t size, const char *src)
{
    if (!src) src = "";
    strlcpy(dst, src, size);
}

static bool sensor_gpio_allowed(int pin)
{
    return pin == 13 || pin == 14 || pin == 16 || pin == 17 || pin == 32 || pin == 33;
}

static void normalize_sensor_rule(sensor_rule_t *rule)
{
    if (rule->type < SENSOR_DS18B20 || rule->type > SENSOR_DHT11) rule->type = SENSOR_DS18B20;
    if (rule->poll_ms < 1000) rule->poll_ms = 1000;
    if (rule->temp_unit < TEMP_C || rule->temp_unit > TEMP_K) rule->temp_unit = TEMP_C;
    if (rule->press_unit < PRESS_HPA || rule->press_unit > PRESS_BAR) rule->press_unit = PRESS_HPA;
    if (rule->type == SENSOR_BME280 || rule->type == SENSOR_SHT3X) {
        if (!sensor_gpio_allowed(rule->sda)) rule->sda = 32;
        if (!sensor_gpio_allowed(rule->scl) || rule->scl == rule->sda) rule->scl = 33;
        if (rule->addr <= 0 || rule->addr > 0x7f) rule->addr = rule->type == SENSOR_SHT3X ? 0x44 : 0x76;
        if (rule->type == SENSOR_BME280) {
            if (rule->resolution != 1 && rule->resolution != 2 && rule->resolution != 4 && rule->resolution != 8 && rule->resolution != 16) {
                rule->resolution = 1;
            }
        } else if (rule->resolution < 0 || rule->resolution > 2) {
            rule->resolution = 0;
        }
    } else {
        if (!sensor_gpio_allowed(rule->pin)) rule->pin = 32;
        if (rule->type == SENSOR_DS18B20) {
            if (rule->resolution < 9 || rule->resolution > 12) rule->resolution = 12;
        } else if (rule->resolution <= 0) {
            rule->resolution = 1;
        }
    }
}

static void set_defaults(void)
{
    memset(&g_cfg, 0, sizeof(g_cfg));
    copy_str(g_cfg.host, sizeof(g_cfg.host), "kamami-gateway");
    g_cfg.ui_lang = 0;
    g_cfg.net_mode = NET_ETH_WIFI_AP;
    g_cfg.dhcp = true;
    copy_str(g_cfg.ip, sizeof(g_cfg.ip), "192.168.1.177");
    copy_str(g_cfg.gateway, sizeof(g_cfg.gateway), "192.168.1.1");
    copy_str(g_cfg.subnet, sizeof(g_cfg.subnet), "255.255.255.0");
    copy_str(g_cfg.dns, sizeof(g_cfg.dns), "1.1.1.1");
    copy_str(g_cfg.ap_ssid, sizeof(g_cfg.ap_ssid), "KAMAMI-Gateway");
    copy_str(g_cfg.ap_pass, sizeof(g_cfg.ap_pass), "kamami1234");
    g_cfg.espnow_enabled = false;
    g_cfg.espnow_channel = 1;
    copy_str(g_cfg.espnow_peer, sizeof(g_cfg.espnow_peer), "FF:FF:FF:FF:FF:FF");
    copy_str(g_cfg.espnow_rx_topic, sizeof(g_cfg.espnow_rx_topic), "kamami/gateway/espnow/rx");
    copy_str(g_cfg.espnow_tx_topic, sizeof(g_cfg.espnow_tx_topic), "kamami/gateway/espnow/tx");
    g_cfg.mqtt_mode = MQTT_BROKER;
    copy_str(g_cfg.mqtt_host, sizeof(g_cfg.mqtt_host), "192.168.1.10");
    g_cfg.mqtt_port = 1883;
    g_cfg.broker_port = 1883;
    copy_str(g_cfg.base_topic, sizeof(g_cfg.base_topic), "kamami/gateway");
    g_cfg.rules[0].enabled = true;
    g_cfg.rules[0].pin = 2;
    g_cfg.rules[0].retain = true;
    copy_str(g_cfg.rules[0].name, sizeof(g_cfg.rules[0].name), "LED");
    copy_str(g_cfg.rules[0].topic, sizeof(g_cfg.rules[0].topic), "kamami/gateway/gpio/2/set");
    for (int i = 0; i < MAX_MODBUS_RULES; i++) {
        g_cfg.modbus[i].port = 502;
        g_cfg.modbus[i].unit = 1;
        g_cfg.modbus[i].poll_ms = 1000;
        copy_str(g_cfg.modbus[i].host, sizeof(g_cfg.modbus[i].host), "192.168.1.100");
    }
    for (int i = 0; i < MAX_SENSOR_RULES; i++) {
        g_cfg.sensors[i].type = SENSOR_DS18B20;
        g_cfg.sensors[i].pin = 32;
        g_cfg.sensors[i].sda = 32;
        g_cfg.sensors[i].scl = 33;
        g_cfg.sensors[i].addr = 0x76;
        g_cfg.sensors[i].poll_ms = 5000;
        g_cfg.sensors[i].resolution = 12;
        g_cfg.sensors[i].temp_unit = TEMP_C;
        g_cfg.sensors[i].press_unit = PRESS_HPA;
    }
}

static bool parse_bool_payload(const char *data, int len)
{
    char tmp[12] = {0};
    int n = len < (int)sizeof(tmp) - 1 ? len : (int)sizeof(tmp) - 1;
    memcpy(tmp, data, n);
    for (int i = 0; tmp[i]; i++) if (tmp[i] >= 'A' && tmp[i] <= 'Z') tmp[i] += 32;
    return strcmp(tmp, "1") == 0 || strcmp(tmp, "on") == 0 || strcmp(tmp, "true") == 0 || strcmp(tmp, "high") == 0;
}

static bool payload_known(const char *data, int len)
{
    char tmp[12] = {0};
    int n = len < (int)sizeof(tmp) - 1 ? len : (int)sizeof(tmp) - 1;
    memcpy(tmp, data, n);
    for (int i = 0; tmp[i]; i++) if (tmp[i] >= 'A' && tmp[i] <= 'Z') tmp[i] += 32;
    return strcmp(tmp, "1") == 0 || strcmp(tmp, "on") == 0 || strcmp(tmp, "true") == 0 || strcmp(tmp, "high") == 0 ||
           strcmp(tmp, "0") == 0 || strcmp(tmp, "off") == 0 || strcmp(tmp, "false") == 0 || strcmp(tmp, "low") == 0;
}

static void publish_gpio_state(const gpio_rule_t *rule, bool value)
{
    if (!g_mqtt_online || !g_mqtt) return;
    char topic[128];
    snprintf(topic, sizeof(topic), "%s/gpio/%d/state", g_cfg.base_topic, rule->pin);
    esp_mqtt_client_publish(g_mqtt, topic, value ? "ON" : "OFF", 0, 0, rule->retain);
}

static void mqtt_publish_text(const char *topic, const char *payload, bool retain)
{
    if (!topic || !topic[0] || !g_mqtt_online || !g_mqtt) return;
    esp_mqtt_client_publish(g_mqtt, topic, payload, 0, 0, retain);
}

static int modbus_connect(const char *host, int port)
{
    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%d", port);
    struct addrinfo hints = {
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM,
    };
    struct addrinfo *res = NULL;
    if (getaddrinfo(host, port_str, &hints, &res) != 0 || !res) return -1;
    int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0) {
        freeaddrinfo(res);
        return -1;
    }
    struct timeval timeout = { .tv_sec = 1, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if (connect(sock, res->ai_addr, res->ai_addrlen) != 0) {
        close(sock);
        sock = -1;
    }
    freeaddrinfo(res);
    return sock;
}

static bool recv_exact(int sock, uint8_t *buf, int len)
{
    int got = 0;
    while (got < len) {
        int n = recv(sock, buf + got, len - got, 0);
        if (n <= 0) return false;
        got += n;
    }
    return true;
}

static bool modbus_read_holding(const modbus_rule_t *rule, uint16_t *value)
{
    static uint16_t tid = 1;
    int sock = modbus_connect(rule->host, rule->port);
    if (sock < 0) return false;
    uint16_t tx = tid++;
    uint8_t req[12] = {
        tx >> 8, tx & 0xff, 0, 0, 0, 6, rule->unit & 0xff, 3,
        (rule->reg >> 8) & 0xff, rule->reg & 0xff, 0, 1
    };
    bool ok = send(sock, req, sizeof(req), 0) == sizeof(req);
    uint8_t hdr[9];
    ok = ok && recv_exact(sock, hdr, sizeof(hdr));
    if (ok && hdr[7] == 3 && hdr[8] == 2) {
        uint8_t data[2];
        ok = recv_exact(sock, data, 2);
        if (ok) *value = ((uint16_t)data[0] << 8) | data[1];
    } else {
        ok = false;
    }
    close(sock);
    return ok;
}

static bool modbus_write_single(const modbus_rule_t *rule, uint16_t value)
{
    static uint16_t tid = 1000;
    int sock = modbus_connect(rule->host, rule->port);
    if (sock < 0) return false;
    uint16_t tx = tid++;
    uint8_t req[12] = {
        tx >> 8, tx & 0xff, 0, 0, 0, 6, rule->unit & 0xff, 6,
        (rule->reg >> 8) & 0xff, rule->reg & 0xff, (value >> 8) & 0xff, value & 0xff
    };
    uint8_t resp[12];
    bool ok = send(sock, req, sizeof(req), 0) == sizeof(req) && recv_exact(sock, resp, sizeof(resp));
    ok = ok && resp[7] == 6 && resp[8] == req[8] && resp[9] == req[9];
    close(sock);
    return ok;
}

static bool parse_u16_payload(const char *data, int len, uint16_t *out)
{
    char tmp[24] = {0};
    int n = len < (int)sizeof(tmp) - 1 ? len : (int)sizeof(tmp) - 1;
    memcpy(tmp, data, n);
    char *end = NULL;
    long value = strtol(tmp, &end, 0);
    if (end == tmp || value < 0 || value > 65535) return false;
    *out = (uint16_t)value;
    return true;
}

static void one_wire_release(int pin)
{
    gpio_set_direction((gpio_num_t)pin, GPIO_MODE_INPUT);
    gpio_set_pull_mode((gpio_num_t)pin, GPIO_PULLUP_ONLY);
}

static void one_wire_low(int pin)
{
    gpio_set_direction((gpio_num_t)pin, GPIO_MODE_OUTPUT);
    gpio_set_level((gpio_num_t)pin, 0);
}

static bool one_wire_reset(int pin)
{
    one_wire_low(pin);
    esp_rom_delay_us(480);
    one_wire_release(pin);
    esp_rom_delay_us(70);
    bool present = gpio_get_level((gpio_num_t)pin) == 0;
    esp_rom_delay_us(410);
    return present;
}

static void one_wire_write_bit(int pin, int bit)
{
    one_wire_low(pin);
    if (bit) {
        esp_rom_delay_us(6);
        one_wire_release(pin);
        esp_rom_delay_us(64);
    } else {
        esp_rom_delay_us(60);
        one_wire_release(pin);
        esp_rom_delay_us(10);
    }
}

static int one_wire_read_bit(int pin)
{
    one_wire_low(pin);
    esp_rom_delay_us(6);
    one_wire_release(pin);
    esp_rom_delay_us(9);
    int bit = gpio_get_level((gpio_num_t)pin);
    esp_rom_delay_us(55);
    return bit;
}

static void one_wire_write_byte(int pin, uint8_t value)
{
    for (int i = 0; i < 8; i++) one_wire_write_bit(pin, (value >> i) & 1);
}

static uint8_t one_wire_read_byte(int pin)
{
    uint8_t value = 0;
    for (int i = 0; i < 8; i++) value |= one_wire_read_bit(pin) << i;
    return value;
}

static int hex_nibble(char ch)
{
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

static bool parse_rom64(const char *text, uint8_t rom[8])
{
    if (!text || strlen(text) != 16) return false;
    for (int i = 0; i < 8; i++) {
        int hi = hex_nibble(text[i * 2]);
        int lo = hex_nibble(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        rom[i] = (hi << 4) | lo;
    }
    return true;
}

static bool one_wire_read_rom(int pin, char *out, size_t out_size)
{
    if (!out || out_size < 17 || !one_wire_reset(pin)) return false;
    one_wire_write_byte(pin, 0x33);
    uint8_t rom[8];
    for (int i = 0; i < 8; i++) rom[i] = one_wire_read_byte(pin);
    snprintf(out, out_size, "%02X%02X%02X%02X%02X%02X%02X%02X",
             rom[0], rom[1], rom[2], rom[3], rom[4], rom[5], rom[6], rom[7]);
    return rom[0] != 0x00 && rom[0] != 0xff;
}

static void one_wire_select(int pin, const char *rom_text)
{
    uint8_t rom[8];
    if (parse_rom64(rom_text, rom)) {
        one_wire_write_byte(pin, 0x55);
        for (int i = 0; i < 8; i++) one_wire_write_byte(pin, rom[i]);
    } else {
        one_wire_write_byte(pin, 0xcc);
    }
}

static bool ds18b20_read(int pin, const char *rom_text, int resolution, float *temp_c)
{
    if (resolution < 9 || resolution > 12) resolution = 12;
    uint8_t cfg = 0x1f | ((resolution - 9) << 5);
    if (!one_wire_reset(pin)) return false;
    one_wire_select(pin, rom_text);
    one_wire_write_byte(pin, 0x4e);
    one_wire_write_byte(pin, 0x7f);
    one_wire_write_byte(pin, 0x80);
    one_wire_write_byte(pin, cfg);

    if (!one_wire_reset(pin)) return false;
    one_wire_select(pin, rom_text);
    one_wire_write_byte(pin, 0x44);
    int wait_ms = resolution == 9 ? 100 : (resolution == 10 ? 200 : (resolution == 11 ? 400 : 750));
    vTaskDelay(pdMS_TO_TICKS(wait_ms));
    if (!one_wire_reset(pin)) return false;
    one_wire_select(pin, rom_text);
    one_wire_write_byte(pin, 0xbe);
    uint8_t scratch[9];
    for (int i = 0; i < 9; i++) scratch[i] = one_wire_read_byte(pin);
    int16_t raw = (int16_t)((scratch[1] << 8) | scratch[0]);
    *temp_c = raw / 16.0f;
    return raw != 0x7fff && raw != (int16_t)0x8000;
}

static void swi2c_release(int pin)
{
    gpio_set_direction((gpio_num_t)pin, GPIO_MODE_INPUT);
    gpio_set_pull_mode((gpio_num_t)pin, GPIO_PULLUP_ONLY);
}

static void swi2c_low(int pin)
{
    gpio_set_direction((gpio_num_t)pin, GPIO_MODE_OUTPUT);
    gpio_set_level((gpio_num_t)pin, 0);
}

static void swi2c_delay(void)
{
    esp_rom_delay_us(5);
}

static void swi2c_start(int sda, int scl)
{
    swi2c_release(sda);
    swi2c_release(scl);
    swi2c_delay();
    swi2c_low(sda);
    swi2c_delay();
    swi2c_low(scl);
}

static void swi2c_stop(int sda, int scl)
{
    swi2c_low(sda);
    swi2c_release(scl);
    swi2c_delay();
    swi2c_release(sda);
    swi2c_delay();
}

static bool swi2c_write_byte(int sda, int scl, uint8_t value)
{
    for (int i = 7; i >= 0; i--) {
        if ((value >> i) & 1) swi2c_release(sda); else swi2c_low(sda);
        swi2c_release(scl);
        swi2c_delay();
        swi2c_low(scl);
        swi2c_delay();
    }
    swi2c_release(sda);
    swi2c_release(scl);
    swi2c_delay();
    bool ack = gpio_get_level((gpio_num_t)sda) == 0;
    swi2c_low(scl);
    return ack;
}

static uint8_t swi2c_read_byte(int sda, int scl, bool ack)
{
    uint8_t value = 0;
    swi2c_release(sda);
    for (int i = 7; i >= 0; i--) {
        swi2c_release(scl);
        swi2c_delay();
        if (gpio_get_level((gpio_num_t)sda)) value |= 1 << i;
        swi2c_low(scl);
        swi2c_delay();
    }
    if (ack) swi2c_low(sda); else swi2c_release(sda);
    swi2c_release(scl);
    swi2c_delay();
    swi2c_low(scl);
    swi2c_release(sda);
    return value;
}

static bool swi2c_write_reg(int sda, int scl, int addr, uint8_t reg, uint8_t value)
{
    swi2c_start(sda, scl);
    bool ok = swi2c_write_byte(sda, scl, (addr << 1) | 0) &&
              swi2c_write_byte(sda, scl, reg) &&
              swi2c_write_byte(sda, scl, value);
    swi2c_stop(sda, scl);
    return ok;
}

static bool swi2c_probe(int sda, int scl, int addr)
{
    swi2c_start(sda, scl);
    bool ok = swi2c_write_byte(sda, scl, (addr << 1) | 0);
    swi2c_stop(sda, scl);
    return ok;
}

static bool swi2c_read_reg(int sda, int scl, int addr, uint8_t reg, uint8_t *buf, int len)
{
    swi2c_start(sda, scl);
    bool ok = swi2c_write_byte(sda, scl, (addr << 1) | 0) &&
              swi2c_write_byte(sda, scl, reg);
    if (!ok) {
        swi2c_stop(sda, scl);
        return false;
    }
    swi2c_start(sda, scl);
    ok = swi2c_write_byte(sda, scl, (addr << 1) | 1);
    if (!ok) {
        swi2c_stop(sda, scl);
        return false;
    }
    for (int i = 0; i < len; i++) buf[i] = swi2c_read_byte(sda, scl, i < len - 1);
    swi2c_stop(sda, scl);
    return true;
}

static bool sht3x_read(int sda, int scl, int addr, int repeatability, float *temp_c, float *hum)
{
    uint8_t data[6];
    uint8_t cmd_lsb = repeatability == 2 ? 0x16 : (repeatability == 1 ? 0x0b : 0x00);
    swi2c_start(sda, scl);
    bool ok = swi2c_write_byte(sda, scl, (addr << 1) | 0) &&
              swi2c_write_byte(sda, scl, 0x24) &&
              swi2c_write_byte(sda, scl, cmd_lsb);
    swi2c_stop(sda, scl);
    if (!ok) return false;
    vTaskDelay(pdMS_TO_TICKS(20));
    swi2c_start(sda, scl);
    ok = swi2c_write_byte(sda, scl, (addr << 1) | 1);
    if (!ok) {
        swi2c_stop(sda, scl);
        return false;
    }
    for (int i = 0; i < 6; i++) data[i] = swi2c_read_byte(sda, scl, i < 5);
    swi2c_stop(sda, scl);
    uint16_t raw_t = ((uint16_t)data[0] << 8) | data[1];
    uint16_t raw_h = ((uint16_t)data[3] << 8) | data[4];
    *temp_c = -45.0f + 175.0f * raw_t / 65535.0f;
    *hum = 100.0f * raw_h / 65535.0f;
    return true;
}

static uint16_t u16_le(const uint8_t *p) { return ((uint16_t)p[1] << 8) | p[0]; }
static int16_t s16_le(const uint8_t *p) { return (int16_t)u16_le(p); }

static uint8_t bme280_osrs_code(int oversampling)
{
    switch (oversampling) {
        case 16: return 5;
        case 8: return 4;
        case 4: return 3;
        case 2: return 2;
        default: return 1;
    }
}

static bool bme280_read(int sda, int scl, int addr, int oversampling, float *temp_c, float *hum, float *press_hpa)
{
    uint8_t id = 0;
    if (!swi2c_read_reg(sda, scl, addr, 0xd0, &id, 1) || id != 0x60) return false;

    uint8_t cal1[26], cal2[7], raw[8];
    if (!swi2c_read_reg(sda, scl, addr, 0x88, cal1, sizeof(cal1))) return false;
    if (!swi2c_read_reg(sda, scl, addr, 0xe1, cal2, sizeof(cal2))) return false;

    uint16_t dig_T1 = u16_le(&cal1[0]);
    int16_t dig_T2 = s16_le(&cal1[2]);
    int16_t dig_T3 = s16_le(&cal1[4]);
    uint16_t dig_P1 = u16_le(&cal1[6]);
    int16_t dig_P2 = s16_le(&cal1[8]);
    int16_t dig_P3 = s16_le(&cal1[10]);
    int16_t dig_P4 = s16_le(&cal1[12]);
    int16_t dig_P5 = s16_le(&cal1[14]);
    int16_t dig_P6 = s16_le(&cal1[16]);
    int16_t dig_P7 = s16_le(&cal1[18]);
    int16_t dig_P8 = s16_le(&cal1[20]);
    int16_t dig_P9 = s16_le(&cal1[22]);
    uint8_t dig_H1 = cal1[25];
    int16_t dig_H2 = s16_le(&cal2[0]);
    uint8_t dig_H3 = cal2[2];
    int16_t dig_H4 = (int16_t)((cal2[3] << 4) | (cal2[4] & 0x0f));
    int16_t dig_H5 = (int16_t)((cal2[5] << 4) | (cal2[4] >> 4));
    int8_t dig_H6 = (int8_t)cal2[6];
    if (dig_H4 & 0x0800) dig_H4 |= 0xf000;
    if (dig_H5 & 0x0800) dig_H5 |= 0xf000;

    uint8_t osrs = bme280_osrs_code(oversampling);
    if (!swi2c_write_reg(sda, scl, addr, 0xf2, osrs)) return false;
    if (!swi2c_write_reg(sda, scl, addr, 0xf4, (osrs << 5) | (osrs << 2) | 1)) return false;
    vTaskDelay(pdMS_TO_TICKS(30 + osrs * 20));
    if (!swi2c_read_reg(sda, scl, addr, 0xf7, raw, sizeof(raw))) return false;

    int32_t adc_P = ((int32_t)raw[0] << 12) | ((int32_t)raw[1] << 4) | (raw[2] >> 4);
    int32_t adc_T = ((int32_t)raw[3] << 12) | ((int32_t)raw[4] << 4) | (raw[5] >> 4);
    int32_t adc_H = ((int32_t)raw[6] << 8) | raw[7];

    int32_t var1 = ((((adc_T >> 3) - ((int32_t)dig_T1 << 1))) * dig_T2) >> 11;
    int32_t var2 = (((((adc_T >> 4) - (int32_t)dig_T1) * ((adc_T >> 4) - (int32_t)dig_T1)) >> 12) * dig_T3) >> 14;
    int32_t t_fine = var1 + var2;
    *temp_c = ((t_fine * 5 + 128) >> 8) / 100.0f;

    int64_t p1 = (int64_t)t_fine - 128000;
    int64_t p2 = p1 * p1 * dig_P6;
    p2 += (p1 * dig_P5) << 17;
    p2 += ((int64_t)dig_P4) << 35;
    p1 = ((p1 * p1 * dig_P3) >> 8) + ((p1 * dig_P2) << 12);
    p1 = (((((int64_t)1) << 47) + p1)) * dig_P1 >> 33;
    if (p1 == 0) return false;
    int64_t p = 1048576 - adc_P;
    p = (((p << 31) - p2) * 3125) / p1;
    p1 = (dig_P9 * (p >> 13) * (p >> 13)) >> 25;
    p2 = (dig_P8 * p) >> 19;
    p = ((p + p1 + p2) >> 8) + (((int64_t)dig_P7) << 4);
    *press_hpa = (p / 256.0f) / 100.0f;

    int32_t h = t_fine - 76800;
    h = (((((adc_H << 14) - (((int32_t)dig_H4) << 20) - (((int32_t)dig_H5) * h)) + 16384) >> 15) *
         (((((((h * dig_H6) >> 10) * (((h * dig_H3) >> 11) + 32768)) >> 10) + 2097152) * dig_H2 + 8192) >> 14));
    h = h - (((((h >> 15) * (h >> 15)) >> 7) * dig_H1) >> 4);
    if (h < 0) h = 0;
    if (h > 419430400) h = 419430400;
    *hum = (h >> 12) / 1024.0f;
    return true;
}

static int dht_wait_while(int pin, int level, int timeout_us)
{
    int64_t start = esp_timer_get_time();
    while (gpio_get_level((gpio_num_t)pin) == level) {
        if ((int)(esp_timer_get_time() - start) > timeout_us) return -1;
    }
    return (int)(esp_timer_get_time() - start);
}

static bool dht_read(int pin, bool dht11, float *temp_c, float *hum)
{
    uint8_t data[5] = {0};
    gpio_set_direction((gpio_num_t)pin, GPIO_MODE_OUTPUT);
    gpio_set_level((gpio_num_t)pin, 0);
    vTaskDelay(pdMS_TO_TICKS(dht11 ? 20 : 2));
    gpio_set_level((gpio_num_t)pin, 1);
    esp_rom_delay_us(30);
    gpio_set_direction((gpio_num_t)pin, GPIO_MODE_INPUT);
    gpio_set_pull_mode((gpio_num_t)pin, GPIO_PULLUP_ONLY);

    if (dht_wait_while(pin, 1, 100) < 0) return false;
    if (dht_wait_while(pin, 0, 100) < 0) return false;
    if (dht_wait_while(pin, 1, 100) < 0) return false;

    for (int i = 0; i < 40; i++) {
        if (dht_wait_while(pin, 0, 100) < 0) return false;
        int high = dht_wait_while(pin, 1, 120);
        if (high < 0) return false;
        data[i / 8] <<= 1;
        if (high > 50) data[i / 8] |= 1;
    }
    if (((data[0] + data[1] + data[2] + data[3]) & 0xff) != data[4]) return false;
    if (dht11) {
        *hum = data[0] + data[1] / 10.0f;
        *temp_c = data[2] + data[3] / 10.0f;
    } else {
        *hum = (((uint16_t)data[0] << 8) | data[1]) / 10.0f;
        int16_t raw_t = (((uint16_t)(data[2] & 0x7f) << 8) | data[3]);
        *temp_c = raw_t / 10.0f;
        if (data[2] & 0x80) *temp_c = -*temp_c;
    }
    return true;
}

static float apply_temp_unit(float temp_c, temp_unit_t unit)
{
    if (unit == TEMP_F) return temp_c * 9.0f / 5.0f + 32.0f;
    if (unit == TEMP_K) return temp_c + 273.15f;
    return temp_c;
}

static const char *temp_unit_label(temp_unit_t unit)
{
    if (unit == TEMP_F) return "F";
    if (unit == TEMP_K) return "K";
    return "C";
}

static float apply_press_unit(float press_hpa, press_unit_t unit)
{
    if (unit == PRESS_PA) return press_hpa * 100.0f;
    if (unit == PRESS_KPA) return press_hpa / 10.0f;
    if (unit == PRESS_BAR) return press_hpa / 1000.0f;
    return press_hpa;
}

static float clamp_humidity(float hum)
{
    if (hum < 0.0f) return 0.0f;
    if (hum > 100.0f) return 100.0f;
    return hum;
}

static const char *press_unit_label(press_unit_t unit)
{
    if (unit == PRESS_PA) return "Pa";
    if (unit == PRESS_KPA) return "kPa";
    if (unit == PRESS_BAR) return "bar";
    return "hPa";
}

static void publish_sensor_reading(sensor_rule_t *rule)
{
    if (!rule->topic[0]) return;
    cJSON *root = cJSON_CreateObject();
    if (rule->name[0]) cJSON_AddStringToObject(root, "name", rule->name);
    cJSON_AddNumberToObject(root, "type", rule->type);
    cJSON_AddStringToObject(root, "temperature_unit", temp_unit_label(rule->temp_unit));
    cJSON_AddStringToObject(root, "pressure_unit", press_unit_label(rule->press_unit));
    if (rule->has_temp) cJSON_AddNumberToObject(root, "temperature", rule->last_temp);
    if (rule->has_hum) cJSON_AddNumberToObject(root, "humidity", rule->last_hum);
    if (rule->has_press) cJSON_AddNumberToObject(root, "pressure", rule->last_press);
    char *json = cJSON_PrintUnformatted(root);
    if (json) {
        mqtt_publish_text(rule->topic, json, false);
        free(json);
    }
    cJSON_Delete(root);

    char topic[128], payload[24];
    if (rule->has_temp) {
        snprintf(topic, sizeof(topic), "%s/temperature", rule->topic);
        snprintf(payload, sizeof(payload), "%.2f", rule->last_temp);
        mqtt_publish_text(topic, payload, false);
    }
    if (rule->has_hum) {
        snprintf(topic, sizeof(topic), "%s/humidity", rule->topic);
        snprintf(payload, sizeof(payload), "%.2f", rule->last_hum);
        mqtt_publish_text(topic, payload, false);
    }
    if (rule->has_press) {
        snprintf(topic, sizeof(topic), "%s/pressure", rule->topic);
        snprintf(payload, sizeof(payload), "%.2f", rule->last_press);
        mqtt_publish_text(topic, payload, false);
    }
}

static void apply_gpio_rule(int idx, bool value, bool publish)
{
    if (idx < 0 || idx >= MAX_GPIO_RULES || !g_cfg.rules[idx].enabled) return;
    gpio_rule_t *r = &g_cfg.rules[idx];
    gpio_set_direction((gpio_num_t)r->pin, GPIO_MODE_OUTPUT);
    gpio_set_level((gpio_num_t)r->pin, r->inverted ? !value : value);
    if (publish) publish_gpio_state(r, value);
}

static void handle_topic_message(const char *topic, const char *data, int len, bool publish)
{
    if (!payload_known(data, len)) return;
    bool value = parse_bool_payload(data, len);
    for (int i = 0; i < MAX_GPIO_RULES; i++) {
        if (g_cfg.rules[i].enabled && strcmp(topic, g_cfg.rules[i].topic) == 0) {
            apply_gpio_rule(i, value, publish);
        }
    }
}

static void handle_modbus_topic_message(const char *topic, const char *data, int len)
{
    uint16_t value;
    if (!parse_u16_payload(data, len, &value)) return;
    for (int i = 0; i < MAX_MODBUS_RULES; i++) {
        modbus_rule_t *rule = &g_cfg.modbus[i];
        if (!rule->enabled || !rule->topic[0]) continue;
        if (rule->direction != MB_TOPIC_TO_REG && rule->direction != MB_BOTH) continue;
        if (strcmp(topic, rule->topic) != 0) continue;
        if (modbus_write_single(rule, value)) {
            rule->last_value = value;
            rule->has_value = true;
        }
    }
}

static bool parse_mac(const char *text, uint8_t mac[6])
{
    if (!text) return false;
    unsigned int b[6];
    if (sscanf(text, "%02x:%02x:%02x:%02x:%02x:%02x", &b[0], &b[1], &b[2], &b[3], &b[4], &b[5]) != 6 &&
        sscanf(text, "%02X:%02X:%02X:%02X:%02X:%02X", &b[0], &b[1], &b[2], &b[3], &b[4], &b[5]) != 6) {
        return false;
    }
    for (int i = 0; i < 6; i++) mac[i] = (uint8_t)b[i];
    return true;
}

static void handle_espnow_topic_message(const char *topic, const char *data, int len)
{
    if (!g_espnow_ready || !g_cfg.espnow_tx_topic[0] || strcmp(topic, g_cfg.espnow_tx_topic) != 0) return;
    uint8_t mac[6];
    if (!parse_mac(g_cfg.espnow_peer, mac)) return;
    esp_err_t err = esp_now_send(mac, (const uint8_t *)data, len);
    if (err != ESP_OK) ESP_LOGW(TAG, "ESP-NOW send failed: %s", esp_err_to_name(err));
}

static cJSON *config_to_json(void)
{
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "fw", FW_NAME);
    cJSON_AddStringToObject(root, "version", FW_VERSION);
    cJSON_AddStringToObject(root, "host", g_cfg.host);
    cJSON_AddNumberToObject(root, "ui_lang", g_cfg.ui_lang);
    cJSON_AddNumberToObject(root, "net_mode", g_cfg.net_mode);
    cJSON_AddBoolToObject(root, "dhcp", g_cfg.dhcp);
    cJSON_AddStringToObject(root, "ip", g_cfg.ip);
    cJSON_AddStringToObject(root, "gateway", g_cfg.gateway);
    cJSON_AddStringToObject(root, "subnet", g_cfg.subnet);
    cJSON_AddStringToObject(root, "dns", g_cfg.dns);
    cJSON_AddStringToObject(root, "sta_ssid", g_cfg.sta_ssid);
    cJSON_AddStringToObject(root, "sta_pass", g_cfg.sta_pass);
    cJSON_AddStringToObject(root, "ap_ssid", g_cfg.ap_ssid);
    cJSON_AddStringToObject(root, "ap_pass", g_cfg.ap_pass);
    cJSON_AddBoolToObject(root, "espnow_enabled", g_cfg.espnow_enabled);
    cJSON_AddNumberToObject(root, "espnow_channel", g_cfg.espnow_channel);
    cJSON_AddStringToObject(root, "espnow_peer", g_cfg.espnow_peer);
    cJSON_AddStringToObject(root, "espnow_rx_topic", g_cfg.espnow_rx_topic);
    cJSON_AddStringToObject(root, "espnow_tx_topic", g_cfg.espnow_tx_topic);
    cJSON_AddNumberToObject(root, "mqtt_mode", g_cfg.mqtt_mode);
    cJSON_AddStringToObject(root, "mqtt_host", g_cfg.mqtt_host);
    cJSON_AddNumberToObject(root, "mqtt_port", g_cfg.mqtt_port);
    cJSON_AddStringToObject(root, "mqtt_user", g_cfg.mqtt_user);
    cJSON_AddStringToObject(root, "mqtt_pass", g_cfg.mqtt_pass);
    cJSON_AddStringToObject(root, "base_topic", g_cfg.base_topic);
    cJSON_AddNumberToObject(root, "broker_port", g_cfg.broker_port);
    cJSON_AddStringToObject(root, "broker_user", g_cfg.broker_user);
    cJSON_AddStringToObject(root, "broker_pass", g_cfg.broker_pass);
    cJSON *rules = cJSON_AddArrayToObject(root, "rules");
    for (int i = 0; i < MAX_GPIO_RULES; i++) {
        if (!g_cfg.rules[i].enabled && g_cfg.rules[i].topic[0] == 0) continue;
        cJSON *r = cJSON_CreateObject();
        cJSON_AddBoolToObject(r, "enabled", g_cfg.rules[i].enabled);
        cJSON_AddNumberToObject(r, "pin", g_cfg.rules[i].pin);
        cJSON_AddBoolToObject(r, "inverted", g_cfg.rules[i].inverted);
        cJSON_AddBoolToObject(r, "retain", g_cfg.rules[i].retain);
        cJSON_AddStringToObject(r, "name", g_cfg.rules[i].name);
        cJSON_AddStringToObject(r, "topic", g_cfg.rules[i].topic);
        cJSON_AddItemToArray(rules, r);
    }
    cJSON *modbus = cJSON_AddArrayToObject(root, "modbus_tcp");
    for (int i = 0; i < MAX_MODBUS_RULES; i++) {
        if (!g_cfg.modbus[i].enabled && g_cfg.modbus[i].topic[0] == 0) continue;
        cJSON *m = cJSON_CreateObject();
        cJSON_AddBoolToObject(m, "enabled", g_cfg.modbus[i].enabled);
        cJSON_AddNumberToObject(m, "direction", g_cfg.modbus[i].direction);
        cJSON_AddStringToObject(m, "name", g_cfg.modbus[i].name);
        cJSON_AddStringToObject(m, "topic", g_cfg.modbus[i].topic);
        cJSON_AddStringToObject(m, "host", g_cfg.modbus[i].host);
        cJSON_AddNumberToObject(m, "port", g_cfg.modbus[i].port);
        cJSON_AddNumberToObject(m, "unit", g_cfg.modbus[i].unit);
        cJSON_AddNumberToObject(m, "reg", g_cfg.modbus[i].reg);
        cJSON_AddNumberToObject(m, "poll_ms", g_cfg.modbus[i].poll_ms);
        cJSON_AddNumberToObject(m, "last_value", g_cfg.modbus[i].last_value);
        cJSON_AddBoolToObject(m, "has_value", g_cfg.modbus[i].has_value);
        cJSON_AddItemToArray(modbus, m);
    }
    cJSON *sensors = cJSON_AddArrayToObject(root, "sensors");
    for (int i = 0; i < MAX_SENSOR_RULES; i++) {
        if (!g_cfg.sensors[i].enabled && g_cfg.sensors[i].topic[0] == 0) continue;
        cJSON *s = cJSON_CreateObject();
        cJSON_AddBoolToObject(s, "enabled", g_cfg.sensors[i].enabled);
        cJSON_AddNumberToObject(s, "type", g_cfg.sensors[i].type);
        cJSON_AddStringToObject(s, "name", g_cfg.sensors[i].name);
        cJSON_AddStringToObject(s, "topic", g_cfg.sensors[i].topic);
        cJSON_AddStringToObject(s, "rom", g_cfg.sensors[i].rom);
        cJSON_AddNumberToObject(s, "pin", g_cfg.sensors[i].pin);
        cJSON_AddNumberToObject(s, "sda", g_cfg.sensors[i].sda);
        cJSON_AddNumberToObject(s, "scl", g_cfg.sensors[i].scl);
        cJSON_AddNumberToObject(s, "addr", g_cfg.sensors[i].addr);
        cJSON_AddNumberToObject(s, "poll_ms", g_cfg.sensors[i].poll_ms);
        cJSON_AddNumberToObject(s, "resolution", g_cfg.sensors[i].resolution);
        cJSON_AddNumberToObject(s, "temp_unit", g_cfg.sensors[i].temp_unit);
        cJSON_AddNumberToObject(s, "press_unit", g_cfg.sensors[i].press_unit);
        cJSON_AddNumberToObject(s, "temp_offset", g_cfg.sensors[i].temp_offset);
        cJSON_AddNumberToObject(s, "hum_offset", g_cfg.sensors[i].hum_offset);
        cJSON_AddNumberToObject(s, "press_offset", g_cfg.sensors[i].press_offset);
        cJSON_AddStringToObject(s, "temperature_unit_label", temp_unit_label(g_cfg.sensors[i].temp_unit));
        cJSON_AddStringToObject(s, "pressure_unit_label", press_unit_label(g_cfg.sensors[i].press_unit));
        cJSON_AddNumberToObject(s, "last_temp", g_cfg.sensors[i].last_temp);
        cJSON_AddNumberToObject(s, "last_hum", g_cfg.sensors[i].last_hum);
        cJSON_AddNumberToObject(s, "last_press", g_cfg.sensors[i].last_press);
        cJSON_AddBoolToObject(s, "has_temp", g_cfg.sensors[i].has_temp);
        cJSON_AddBoolToObject(s, "has_hum", g_cfg.sensors[i].has_hum);
        cJSON_AddBoolToObject(s, "has_press", g_cfg.sensors[i].has_press);
        cJSON_AddItemToArray(sensors, s);
    }
    return root;
}

static void parse_config_json(const char *json)
{
    cJSON *root = cJSON_Parse(json);
    if (!root) return;
    cJSON *v;
    if ((v = cJSON_GetObjectItem(root, "host"))) copy_str(g_cfg.host, sizeof(g_cfg.host), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "ui_lang"))) g_cfg.ui_lang = v->valueint ? 1 : 0;
    if ((v = cJSON_GetObjectItem(root, "net_mode"))) g_cfg.net_mode = v->valueint;
    if ((v = cJSON_GetObjectItem(root, "dhcp"))) g_cfg.dhcp = cJSON_IsTrue(v);
    if ((v = cJSON_GetObjectItem(root, "ip"))) copy_str(g_cfg.ip, sizeof(g_cfg.ip), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "gateway"))) copy_str(g_cfg.gateway, sizeof(g_cfg.gateway), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "subnet"))) copy_str(g_cfg.subnet, sizeof(g_cfg.subnet), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "dns"))) copy_str(g_cfg.dns, sizeof(g_cfg.dns), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "sta_ssid"))) copy_str(g_cfg.sta_ssid, sizeof(g_cfg.sta_ssid), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "sta_pass"))) copy_str(g_cfg.sta_pass, sizeof(g_cfg.sta_pass), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "ap_ssid"))) copy_str(g_cfg.ap_ssid, sizeof(g_cfg.ap_ssid), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "ap_pass"))) copy_str(g_cfg.ap_pass, sizeof(g_cfg.ap_pass), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "espnow_enabled"))) g_cfg.espnow_enabled = cJSON_IsTrue(v);
    if ((v = cJSON_GetObjectItem(root, "espnow_channel"))) g_cfg.espnow_channel = v->valueint;
    if (g_cfg.espnow_channel < 1 || g_cfg.espnow_channel > 13) g_cfg.espnow_channel = 1;
    if ((v = cJSON_GetObjectItem(root, "espnow_peer"))) copy_str(g_cfg.espnow_peer, sizeof(g_cfg.espnow_peer), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "espnow_rx_topic"))) copy_str(g_cfg.espnow_rx_topic, sizeof(g_cfg.espnow_rx_topic), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "espnow_tx_topic"))) copy_str(g_cfg.espnow_tx_topic, sizeof(g_cfg.espnow_tx_topic), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "mqtt_mode"))) g_cfg.mqtt_mode = v->valueint;
    if ((v = cJSON_GetObjectItem(root, "mqtt_host"))) copy_str(g_cfg.mqtt_host, sizeof(g_cfg.mqtt_host), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "mqtt_port"))) g_cfg.mqtt_port = v->valueint;
    if ((v = cJSON_GetObjectItem(root, "mqtt_user"))) copy_str(g_cfg.mqtt_user, sizeof(g_cfg.mqtt_user), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "mqtt_pass"))) copy_str(g_cfg.mqtt_pass, sizeof(g_cfg.mqtt_pass), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "base_topic"))) copy_str(g_cfg.base_topic, sizeof(g_cfg.base_topic), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "broker_port"))) g_cfg.broker_port = v->valueint;
    if (g_cfg.broker_port <= 0 || g_cfg.broker_port > 65535) g_cfg.broker_port = 1883;
    if ((v = cJSON_GetObjectItem(root, "broker_user"))) copy_str(g_cfg.broker_user, sizeof(g_cfg.broker_user), cJSON_GetStringValue(v));
    if ((v = cJSON_GetObjectItem(root, "broker_pass"))) copy_str(g_cfg.broker_pass, sizeof(g_cfg.broker_pass), cJSON_GetStringValue(v));
    cJSON *rules = cJSON_GetObjectItem(root, "rules");
    if (cJSON_IsArray(rules)) {
        memset(g_cfg.rules, 0, sizeof(g_cfg.rules));
        int i = 0;
        cJSON *r;
        cJSON_ArrayForEach(r, rules) {
            if (i >= MAX_GPIO_RULES) break;
            g_cfg.rules[i].enabled = cJSON_IsTrue(cJSON_GetObjectItem(r, "enabled"));
            g_cfg.rules[i].pin = cJSON_GetObjectItem(r, "pin") ? cJSON_GetObjectItem(r, "pin")->valueint : 2;
            g_cfg.rules[i].inverted = cJSON_IsTrue(cJSON_GetObjectItem(r, "inverted"));
            g_cfg.rules[i].retain = cJSON_GetObjectItem(r, "retain") ? cJSON_IsTrue(cJSON_GetObjectItem(r, "retain")) : true;
            copy_str(g_cfg.rules[i].name, sizeof(g_cfg.rules[i].name), cJSON_GetStringValue(cJSON_GetObjectItem(r, "name")));
            copy_str(g_cfg.rules[i].topic, sizeof(g_cfg.rules[i].topic), cJSON_GetStringValue(cJSON_GetObjectItem(r, "topic")));
            i++;
        }
    }
    cJSON *modbus = cJSON_GetObjectItem(root, "modbus_tcp");
    if (cJSON_IsArray(modbus)) {
        memset(g_cfg.modbus, 0, sizeof(g_cfg.modbus));
        int i = 0;
        cJSON *m;
        cJSON_ArrayForEach(m, modbus) {
            if (i >= MAX_MODBUS_RULES) break;
            g_cfg.modbus[i].enabled = cJSON_IsTrue(cJSON_GetObjectItem(m, "enabled"));
            g_cfg.modbus[i].direction = cJSON_GetObjectItem(m, "direction") ? cJSON_GetObjectItem(m, "direction")->valueint : MB_TOPIC_TO_REG;
            copy_str(g_cfg.modbus[i].name, sizeof(g_cfg.modbus[i].name), cJSON_GetStringValue(cJSON_GetObjectItem(m, "name")));
            copy_str(g_cfg.modbus[i].topic, sizeof(g_cfg.modbus[i].topic), cJSON_GetStringValue(cJSON_GetObjectItem(m, "topic")));
            copy_str(g_cfg.modbus[i].host, sizeof(g_cfg.modbus[i].host), cJSON_GetStringValue(cJSON_GetObjectItem(m, "host")));
            g_cfg.modbus[i].port = cJSON_GetObjectItem(m, "port") ? cJSON_GetObjectItem(m, "port")->valueint : 502;
            g_cfg.modbus[i].unit = cJSON_GetObjectItem(m, "unit") ? cJSON_GetObjectItem(m, "unit")->valueint : 1;
            g_cfg.modbus[i].reg = cJSON_GetObjectItem(m, "reg") ? cJSON_GetObjectItem(m, "reg")->valueint : 0;
            g_cfg.modbus[i].poll_ms = cJSON_GetObjectItem(m, "poll_ms") ? cJSON_GetObjectItem(m, "poll_ms")->valueint : 1000;
            if (g_cfg.modbus[i].poll_ms < 200) g_cfg.modbus[i].poll_ms = 200;
            i++;
        }
    }
    cJSON *sensors = cJSON_GetObjectItem(root, "sensors");
    if (cJSON_IsArray(sensors)) {
        memset(g_cfg.sensors, 0, sizeof(g_cfg.sensors));
        int i = 0;
        cJSON *s;
        cJSON_ArrayForEach(s, sensors) {
            if (i >= MAX_SENSOR_RULES) break;
            g_cfg.sensors[i].enabled = cJSON_IsTrue(cJSON_GetObjectItem(s, "enabled"));
            g_cfg.sensors[i].type = cJSON_GetObjectItem(s, "type") ? cJSON_GetObjectItem(s, "type")->valueint : SENSOR_DS18B20;
            copy_str(g_cfg.sensors[i].name, sizeof(g_cfg.sensors[i].name), cJSON_GetStringValue(cJSON_GetObjectItem(s, "name")));
            copy_str(g_cfg.sensors[i].topic, sizeof(g_cfg.sensors[i].topic), cJSON_GetStringValue(cJSON_GetObjectItem(s, "topic")));
            copy_str(g_cfg.sensors[i].rom, sizeof(g_cfg.sensors[i].rom), cJSON_GetStringValue(cJSON_GetObjectItem(s, "rom")));
            g_cfg.sensors[i].pin = cJSON_GetObjectItem(s, "pin") ? cJSON_GetObjectItem(s, "pin")->valueint : 32;
            g_cfg.sensors[i].sda = cJSON_GetObjectItem(s, "sda") ? cJSON_GetObjectItem(s, "sda")->valueint : 32;
            g_cfg.sensors[i].scl = cJSON_GetObjectItem(s, "scl") ? cJSON_GetObjectItem(s, "scl")->valueint : 33;
            g_cfg.sensors[i].addr = cJSON_GetObjectItem(s, "addr") ? cJSON_GetObjectItem(s, "addr")->valueint : 0x76;
            g_cfg.sensors[i].poll_ms = cJSON_GetObjectItem(s, "poll_ms") ? cJSON_GetObjectItem(s, "poll_ms")->valueint : 5000;
            g_cfg.sensors[i].resolution = cJSON_GetObjectItem(s, "resolution") ? cJSON_GetObjectItem(s, "resolution")->valueint : 0;
            g_cfg.sensors[i].temp_unit = cJSON_GetObjectItem(s, "temp_unit") ? cJSON_GetObjectItem(s, "temp_unit")->valueint : TEMP_C;
            g_cfg.sensors[i].press_unit = cJSON_GetObjectItem(s, "press_unit") ? cJSON_GetObjectItem(s, "press_unit")->valueint : PRESS_HPA;
            g_cfg.sensors[i].temp_offset = cJSON_GetObjectItem(s, "temp_offset") ? (float)cJSON_GetObjectItem(s, "temp_offset")->valuedouble : 0.0f;
            g_cfg.sensors[i].hum_offset = cJSON_GetObjectItem(s, "hum_offset") ? (float)cJSON_GetObjectItem(s, "hum_offset")->valuedouble : 0.0f;
            g_cfg.sensors[i].press_offset = cJSON_GetObjectItem(s, "press_offset") ? (float)cJSON_GetObjectItem(s, "press_offset")->valuedouble : 0.0f;
            normalize_sensor_rule(&g_cfg.sensors[i]);
            i++;
        }
    }
    cJSON_Delete(root);
}

static void save_config(void)
{
    cJSON *root = config_to_json();
    char *json = cJSON_PrintUnformatted(root);
    nvs_handle_t nvs;
    if (nvs_open(CONFIG_NAMESPACE, NVS_READWRITE, &nvs) == ESP_OK) {
        nvs_set_str(nvs, CONFIG_KEY, json);
        nvs_commit(nvs);
        nvs_close(nvs);
    }
    free(json);
    cJSON_Delete(root);
}

static void load_config(void)
{
    set_defaults();
    nvs_handle_t nvs;
    if (nvs_open(CONFIG_NAMESPACE, NVS_READONLY, &nvs) != ESP_OK) return;
    size_t len = 0;
    if (nvs_get_str(nvs, CONFIG_KEY, NULL, &len) == ESP_OK && len > 1) {
        char *json = calloc(1, len);
        if (json && nvs_get_str(nvs, CONFIG_KEY, json, &len) == ESP_OK) parse_config_json(json);
        free(json);
    }
    nvs_close(nvs);
}

static bool network_config_changed(const gateway_config_t *old)
{
    return old->net_mode != g_cfg.net_mode ||
           old->dhcp != g_cfg.dhcp ||
           strcmp(old->host, g_cfg.host) != 0 ||
           strcmp(old->ip, g_cfg.ip) != 0 ||
           strcmp(old->gateway, g_cfg.gateway) != 0 ||
           strcmp(old->subnet, g_cfg.subnet) != 0 ||
           strcmp(old->dns, g_cfg.dns) != 0 ||
           strcmp(old->sta_ssid, g_cfg.sta_ssid) != 0 ||
           strcmp(old->sta_pass, g_cfg.sta_pass) != 0 ||
           strcmp(old->ap_ssid, g_cfg.ap_ssid) != 0 ||
           strcmp(old->ap_pass, g_cfg.ap_pass) != 0 ||
           old->espnow_enabled != g_cfg.espnow_enabled ||
           old->espnow_channel != g_cfg.espnow_channel ||
           strcmp(old->espnow_peer, g_cfg.espnow_peer) != 0 ||
           strcmp(old->espnow_rx_topic, g_cfg.espnow_rx_topic) != 0 ||
           strcmp(old->espnow_tx_topic, g_cfg.espnow_tx_topic) != 0;
}

static bool mqtt_config_changed(const gateway_config_t *old)
{
    if (old->mqtt_mode != g_cfg.mqtt_mode ||
        old->mqtt_port != g_cfg.mqtt_port ||
        strcmp(old->mqtt_host, g_cfg.mqtt_host) != 0 ||
        strcmp(old->mqtt_user, g_cfg.mqtt_user) != 0 ||
        strcmp(old->mqtt_pass, g_cfg.mqtt_pass) != 0 ||
        strcmp(old->base_topic, g_cfg.base_topic) != 0 ||
        old->broker_port != g_cfg.broker_port ||
        strcmp(old->broker_user, g_cfg.broker_user) != 0 ||
        strcmp(old->broker_pass, g_cfg.broker_pass) != 0) {
        return true;
    }
    return memcmp(old->rules, g_cfg.rules, sizeof(g_cfg.rules)) != 0 ||
           memcmp(old->modbus, g_cfg.modbus, sizeof(g_cfg.modbus)) != 0;
}

static void stop_mqtt_client(void)
{
    if (!g_mqtt) return;
    esp_mqtt_client_stop(g_mqtt);
    esp_mqtt_client_destroy(g_mqtt);
    g_mqtt = NULL;
    g_mqtt_online = false;
    g_mqtt_local = false;
}

static void stop_broker(void)
{
    if (!g_broker_running) return;
    mosq_broker_stop();
    for (int i = 0; i < 20 && g_broker_running; i++) {
        vTaskDelay(pdMS_TO_TICKS(50));
    }
}

static void apply_runtime_config(const gateway_config_t *old)
{
    for (int i = 0; i < MAX_GPIO_RULES; i++) {
        if (g_cfg.rules[i].enabled) {
            gpio_set_direction((gpio_num_t)g_cfg.rules[i].pin, GPIO_MODE_OUTPUT);
        }
    }

    if (network_config_changed(old)) {
        g_network_restart_required = true;
    }

    if (!mqtt_config_changed(old)) {
        return;
    }

    stop_mqtt_client();
    stop_broker();

    if (g_cfg.mqtt_mode == MQTT_CLIENT) {
        start_mqtt_client();
    } else if (g_cfg.mqtt_mode == MQTT_BROKER) {
        xTaskCreate(broker_task, "mosquitto", 8192, NULL, 5, &g_broker_task);
        vTaskDelay(pdMS_TO_TICKS(250));
        start_mqtt_local_client();
    }
}

static void log_mqtt_message(const char *client, const char *topic, const char *data, int len, int qos, int retain)
{
    if (!g_mqtt_log_lock) return;
    xSemaphoreTake(g_mqtt_log_lock, portMAX_DELAY);
    mqtt_log_entry_t *entry = &g_mqtt_log[g_mqtt_log_next];
    memset(entry, 0, sizeof(*entry));
    entry->seq = ++g_mqtt_log_seq;
    entry->at_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
    entry->len = len;
    entry->qos = qos;
    entry->retain = retain;
    copy_str(entry->client, sizeof(entry->client), client ? client : "");
    copy_str(entry->topic, sizeof(entry->topic), topic ? topic : "");
    int n = len < MQTT_LOG_PAYLOAD_LEN - 1 ? len : MQTT_LOG_PAYLOAD_LEN - 1;
    for (int i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)data[i];
        entry->payload[i] = (ch >= 32 && ch <= 126) ? (char)ch : '.';
    }
    entry->payload[n] = 0;
    g_mqtt_log_next = (g_mqtt_log_next + 1) % MAX_MQTT_LOG;
    if (g_mqtt_log_count < MAX_MQTT_LOG) g_mqtt_log_count++;
    xSemaphoreGive(g_mqtt_log_lock);
}

static esp_err_t root_get(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, (const char *)web_index_html_start, web_index_html_end - web_index_html_start);
}

static esp_err_t config_get(httpd_req_t *req)
{
    cJSON *root = config_to_json();
    char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    esp_err_t ret = httpd_resp_sendstr(req, json);
    free(json);
    cJSON_Delete(root);
    return ret;
}

static esp_err_t config_post(httpd_req_t *req)
{
    char *buf = calloc(1, req->content_len + 1);
    if (!buf) return ESP_ERR_NO_MEM;
    int got = httpd_req_recv(req, buf, req->content_len);
    if (got <= 0) {
        free(buf);
        return ESP_FAIL;
    }
    gateway_config_t old_cfg = g_cfg;
    parse_config_json(buf);
    save_config();
    apply_runtime_config(&old_cfg);
    free(buf);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, g_network_restart_required ? "{\"ok\":true,\"restart_required\":true}" : "{\"ok\":true,\"restart_required\":false}");
    return ESP_OK;
}

static esp_err_t restart_post(httpd_req_t *req)
{
    httpd_resp_set_type(req, "application/json");
    httpd_resp_sendstr(req, "{\"ok\":true}");
    vTaskDelay(pdMS_TO_TICKS(200));
    esp_restart();
    return ESP_OK;
}

static esp_err_t status_get(httpd_req_t *req)
{
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "fw", FW_NAME);
    cJSON_AddStringToObject(root, "version", FW_VERSION);
    cJSON_AddNumberToObject(root, "uptime_s", (esp_timer_get_time() - g_boot_us) / 1000000);
    cJSON_AddStringToObject(root, "ip", g_active_ip);
    cJSON_AddStringToObject(root, "eth", g_eth_online ? "online" : "off");
    cJSON_AddStringToObject(root, "wifi", g_wifi_online ? "online" : (g_cfg.espnow_enabled ? "esp-now radio" : "off"));
    cJSON_AddStringToObject(root, "espnow", g_espnow_ready ? "ready" : (g_cfg.espnow_enabled ? "configured" : "off"));
    char mqtt_status[48];
    if (g_cfg.mqtt_mode == MQTT_BROKER) {
        snprintf(mqtt_status, sizeof(mqtt_status), "mosquitto broker :%d", g_cfg.broker_port);
    } else {
        snprintf(mqtt_status, sizeof(mqtt_status), "%s", g_mqtt_online ? "esp-mqtt online" : "off/offline");
    }
    cJSON_AddStringToObject(root, "mqtt", mqtt_status);
    cJSON_AddNumberToObject(root, "broker_port", g_cfg.broker_port);
    cJSON_AddBoolToObject(root, "restart_required", g_network_restart_required);
    cJSON_AddNumberToObject(root, "free_heap", esp_get_free_heap_size());
    char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    esp_err_t ret = httpd_resp_sendstr(req, json);
    free(json);
    cJSON_Delete(root);
    return ret;
}

static esp_err_t gpio_post(httpd_req_t *req)
{
    char *buf = calloc(1, req->content_len + 1);
    if (!buf) return ESP_ERR_NO_MEM;
    httpd_req_recv(req, buf, req->content_len);
    cJSON *root = cJSON_Parse(buf);
    if (root) {
        int idx = cJSON_GetObjectItem(root, "index") ? cJSON_GetObjectItem(root, "index")->valueint : 0;
        bool value = cJSON_IsTrue(cJSON_GetObjectItem(root, "value")) || (cJSON_GetObjectItem(root, "value") && cJSON_GetObjectItem(root, "value")->valueint);
        apply_gpio_rule(idx, value, true);
        cJSON_Delete(root);
    }
    free(buf);
    httpd_resp_set_type(req, "application/json");
    return httpd_resp_sendstr(req, "{\"ok\":true}");
}

static esp_err_t mqtt_messages_get(httpd_req_t *req)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *items = cJSON_AddArrayToObject(root, "messages");
    cJSON_AddBoolToObject(root, "broker_running", g_broker_running);
    cJSON_AddNumberToObject(root, "broker_port", g_cfg.broker_port);
    xSemaphoreTake(g_mqtt_log_lock, portMAX_DELAY);
    for (int i = 0; i < g_mqtt_log_count; i++) {
        int idx = (g_mqtt_log_next - g_mqtt_log_count + i + MAX_MQTT_LOG) % MAX_MQTT_LOG;
        mqtt_log_entry_t *e = &g_mqtt_log[idx];
        cJSON *row = cJSON_CreateObject();
        cJSON_AddNumberToObject(row, "seq", e->seq);
        cJSON_AddNumberToObject(row, "at_ms", e->at_ms);
        cJSON_AddStringToObject(row, "client", e->client);
        cJSON_AddStringToObject(row, "topic", e->topic);
        cJSON_AddStringToObject(row, "payload", e->payload);
        cJSON_AddNumberToObject(row, "len", e->len);
        cJSON_AddNumberToObject(row, "qos", e->qos);
        cJSON_AddBoolToObject(row, "retain", e->retain);
        cJSON_AddItemToArray(items, row);
    }
    xSemaphoreGive(g_mqtt_log_lock);
    char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    esp_err_t ret = httpd_resp_sendstr(req, json);
    free(json);
    cJSON_Delete(root);
    return ret;
}

static esp_err_t mqtt_messages_delete(httpd_req_t *req)
{
    xSemaphoreTake(g_mqtt_log_lock, portMAX_DELAY);
    memset(g_mqtt_log, 0, sizeof(g_mqtt_log));
    g_mqtt_log_next = 0;
    g_mqtt_log_count = 0;
    xSemaphoreGive(g_mqtt_log_lock);
    httpd_resp_set_type(req, "application/json");
    return httpd_resp_sendstr(req, "{\"ok\":true}");
}

static esp_err_t sensor_scan_post(httpd_req_t *req)
{
    char *buf = calloc(1, req->content_len + 1);
    if (!buf) return ESP_ERR_NO_MEM;
    int got = httpd_req_recv(req, buf, req->content_len);
    if (got <= 0) {
        free(buf);
        return ESP_FAIL;
    }

    cJSON *body = cJSON_Parse(buf);
    free(buf);
    cJSON *root = cJSON_CreateObject();
    cJSON_AddBoolToObject(root, "ok", false);
    if (body) {
        sensor_rule_t rule = {0};
        rule.type = cJSON_GetObjectItem(body, "type") ? cJSON_GetObjectItem(body, "type")->valueint : SENSOR_DS18B20;
        rule.pin = cJSON_GetObjectItem(body, "pin") ? cJSON_GetObjectItem(body, "pin")->valueint : 32;
        rule.sda = cJSON_GetObjectItem(body, "sda") ? cJSON_GetObjectItem(body, "sda")->valueint : 32;
        rule.scl = cJSON_GetObjectItem(body, "scl") ? cJSON_GetObjectItem(body, "scl")->valueint : 33;
        normalize_sensor_rule(&rule);

        if (rule.type == SENSOR_DS18B20) {
            char rom[17] = {0};
            if (one_wire_read_rom(rule.pin, rom, sizeof(rom))) {
                cJSON_ReplaceItemInObject(root, "ok", cJSON_CreateBool(true));
                cJSON_AddStringToObject(root, "rom", rom);
            } else {
                cJSON_AddStringToObject(root, "error", "Nie znaleziono DS18B20 na wybranym GPIO");
            }
        } else if (rule.type == SENSOR_BME280 || rule.type == SENSOR_SHT3X) {
            cJSON *addresses = cJSON_AddArrayToObject(root, "addresses");
            for (int addr = 0x03; addr <= 0x77; addr++) {
                if (swi2c_probe(rule.sda, rule.scl, addr)) {
                    cJSON *item = cJSON_CreateObject();
                    char hex[8];
                    snprintf(hex, sizeof(hex), "0x%02X", addr);
                    cJSON_AddNumberToObject(item, "value", addr);
                    cJSON_AddStringToObject(item, "hex", hex);
                    cJSON_AddItemToArray(addresses, item);
                }
            }
            cJSON_ReplaceItemInObject(root, "ok", cJSON_CreateBool(cJSON_GetArraySize(addresses) > 0));
            if (cJSON_GetArraySize(addresses) == 0) cJSON_AddStringToObject(root, "error", "Brak odpowiedzi na magistrali I2C");
        } else {
            cJSON_AddStringToObject(root, "error", "Ten typ czujnika nie ma adresu do skanowania");
        }
        cJSON_Delete(body);
    } else {
        cJSON_AddStringToObject(root, "error", "Niepoprawny JSON");
    }

    char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    esp_err_t ret = httpd_resp_sendstr(req, json);
    free(json);
    cJSON_Delete(root);
    return ret;
}

static esp_err_t sensors_get(httpd_req_t *req)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *items = cJSON_AddArrayToObject(root, "sensors");
    for (int i = 0; i < MAX_SENSOR_RULES; i++) {
        sensor_rule_t *s = &g_cfg.sensors[i];
        if (!s->enabled && !s->topic[0]) continue;
        cJSON *row = cJSON_CreateObject();
        cJSON_AddNumberToObject(row, "index", i);
        cJSON_AddBoolToObject(row, "enabled", s->enabled);
        cJSON_AddNumberToObject(row, "type", s->type);
        cJSON_AddStringToObject(row, "name", s->name[0] ? s->name : "Sensor");
        cJSON_AddStringToObject(row, "topic", s->topic);
        cJSON_AddStringToObject(row, "temperature_unit_label", temp_unit_label(s->temp_unit));
        cJSON_AddStringToObject(row, "pressure_unit_label", press_unit_label(s->press_unit));
        cJSON_AddNumberToObject(row, "last_temp", s->last_temp);
        cJSON_AddNumberToObject(row, "last_hum", s->last_hum);
        cJSON_AddNumberToObject(row, "last_press", s->last_press);
        cJSON_AddBoolToObject(row, "has_temp", s->has_temp);
        cJSON_AddBoolToObject(row, "has_hum", s->has_hum);
        cJSON_AddBoolToObject(row, "has_press", s->has_press);
        cJSON_AddItemToArray(items, row);
    }
    char *json = cJSON_PrintUnformatted(root);
    httpd_resp_set_type(req, "application/json");
    esp_err_t ret = httpd_resp_sendstr(req, json);
    free(json);
    cJSON_Delete(root);
    return ret;
}

static void start_http(void)
{
    httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
    cfg.max_uri_handlers = 12;
    ESP_ERROR_CHECK(httpd_start(&g_http, &cfg));
    httpd_uri_t root = { .uri = "/", .method = HTTP_GET, .handler = root_get };
    httpd_uri_t api_cfg_get = { .uri = "/api/config", .method = HTTP_GET, .handler = config_get };
    httpd_uri_t api_cfg_post = { .uri = "/api/config", .method = HTTP_POST, .handler = config_post };
    httpd_uri_t api_restart = { .uri = "/api/restart", .method = HTTP_POST, .handler = restart_post };
    httpd_uri_t api_status = { .uri = "/api/status", .method = HTTP_GET, .handler = status_get };
    httpd_uri_t api_gpio = { .uri = "/api/gpio", .method = HTTP_POST, .handler = gpio_post };
    httpd_uri_t api_mqtt_messages = { .uri = "/api/mqtt/messages", .method = HTTP_GET, .handler = mqtt_messages_get };
    httpd_uri_t api_mqtt_messages_clear = { .uri = "/api/mqtt/messages", .method = HTTP_DELETE, .handler = mqtt_messages_delete };
    httpd_uri_t api_sensor_scan = { .uri = "/api/sensor/scan", .method = HTTP_POST, .handler = sensor_scan_post };
    httpd_uri_t api_sensors = { .uri = "/api/sensors", .method = HTTP_GET, .handler = sensors_get };
    httpd_register_uri_handler(g_http, &root);
    httpd_register_uri_handler(g_http, &api_cfg_get);
    httpd_register_uri_handler(g_http, &api_cfg_post);
    httpd_register_uri_handler(g_http, &api_restart);
    httpd_register_uri_handler(g_http, &api_status);
    httpd_register_uri_handler(g_http, &api_gpio);
    httpd_register_uri_handler(g_http, &api_mqtt_messages);
    httpd_register_uri_handler(g_http, &api_mqtt_messages_clear);
    httpd_register_uri_handler(g_http, &api_sensor_scan);
    httpd_register_uri_handler(g_http, &api_sensors);
}

static void ip_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    ip_event_got_ip_t *event = (ip_event_got_ip_t *)data;
    snprintf(g_active_ip, sizeof(g_active_ip), IPSTR, IP2STR(&event->ip_info.ip));
    if (id == IP_EVENT_ETH_GOT_IP) g_eth_online = true;
    if (id == IP_EVENT_STA_GOT_IP) g_wifi_online = true;
    ESP_LOGI(TAG, "IP: %s", g_active_ip);
}

static void wifi_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    if (id == WIFI_EVENT_STA_DISCONNECTED) {
        g_wifi_online = false;
        if (g_wifi_should_connect) esp_wifi_connect();
    }
}

static void eth_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    if (id == ETHERNET_EVENT_DISCONNECTED || id == ETHERNET_EVENT_STOP) g_eth_online = false;
}

static bool parse_ip4(const char *s, esp_ip4_addr_t *out)
{
    out->addr = esp_ip4addr_aton(s);
    return out->addr != 0;
}

static void apply_static_ip(esp_netif_t *netif)
{
    if (g_cfg.dhcp) return;
    esp_netif_dhcpc_stop(netif);
    esp_netif_ip_info_t info = {0};
    parse_ip4(g_cfg.ip, &info.ip);
    parse_ip4(g_cfg.gateway, &info.gw);
    parse_ip4(g_cfg.subnet, &info.netmask);
    esp_netif_set_ip_info(netif, &info);
    esp_netif_dns_info_t dns = {0};
    parse_ip4(g_cfg.dns, &dns.ip.u_addr.ip4);
    dns.ip.type = ESP_IPADDR_TYPE_V4;
    esp_netif_set_dns_info(netif, ESP_NETIF_DNS_MAIN, &dns);
}

static void espnow_recv_cb(const esp_now_recv_info_t *info, const uint8_t *data, int len)
{
    if (!info || !data || len <= 0 || !g_cfg.espnow_rx_topic[0]) return;
    char src[18];
    snprintf(src, sizeof(src), "%02X:%02X:%02X:%02X:%02X:%02X",
             info->src_addr[0], info->src_addr[1], info->src_addr[2],
             info->src_addr[3], info->src_addr[4], info->src_addr[5]);
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "src", src);
    cJSON_AddNumberToObject(root, "len", len);
    if (info->rx_ctrl) cJSON_AddNumberToObject(root, "rssi", info->rx_ctrl->rssi);
    char payload[ESP_NOW_MAX_DATA_LEN + 1];
    int n = len < ESP_NOW_MAX_DATA_LEN ? len : ESP_NOW_MAX_DATA_LEN;
    for (int i = 0; i < n; i++) {
        unsigned char ch = data[i];
        payload[i] = (ch >= 32 && ch <= 126) ? (char)ch : '.';
    }
    payload[n] = 0;
    cJSON_AddStringToObject(root, "payload", payload);
    char *json = cJSON_PrintUnformatted(root);
    if (json) {
        mqtt_publish_text(g_cfg.espnow_rx_topic, json, false);
        free(json);
    }
    cJSON_Delete(root);
}

static void espnow_send_cb(const esp_now_send_info_t *tx_info, esp_now_send_status_t status)
{
    (void)tx_info;
    if (status != ESP_NOW_SEND_SUCCESS) ESP_LOGW(TAG, "ESP-NOW send status=%d", status);
}

static void start_espnow(bool use_ap_if)
{
    if (!g_cfg.espnow_enabled || g_espnow_ready) return;
    ESP_ERROR_CHECK(esp_wifi_set_channel(g_cfg.espnow_channel, WIFI_SECOND_CHAN_NONE));
    ESP_ERROR_CHECK(esp_now_init());
    ESP_ERROR_CHECK(esp_now_register_recv_cb(espnow_recv_cb));
    ESP_ERROR_CHECK(esp_now_register_send_cb(espnow_send_cb));

    uint8_t peer_mac[6];
    if (parse_mac(g_cfg.espnow_peer, peer_mac)) {
        esp_now_peer_info_t peer = {0};
        memcpy(peer.peer_addr, peer_mac, sizeof(peer.peer_addr));
        peer.channel = g_cfg.espnow_channel;
        peer.ifidx = use_ap_if ? WIFI_IF_AP : WIFI_IF_STA;
        peer.encrypt = false;
        esp_err_t err = esp_now_add_peer(&peer);
        if (err != ESP_OK && err != ESP_ERR_ESPNOW_EXIST) {
            ESP_LOGW(TAG, "ESP-NOW add peer failed: %s", esp_err_to_name(err));
        }
    }
    g_espnow_ready = true;
    ESP_LOGI(TAG, "ESP-NOW ready on channel %d", g_cfg.espnow_channel);
}

static void start_wifi(bool sta, bool ap, bool radio_only)
{
    wifi_init_config_t init = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&init));
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, ip_event_handler, NULL));
    g_wifi_should_connect = sta;
    esp_netif_t *sta_netif = NULL;
    if (sta) {
        sta_netif = esp_netif_create_default_wifi_sta();
        apply_static_ip(sta_netif);
    }
    if (ap) esp_netif_create_default_wifi_ap();
    wifi_config_t sta_cfg = {0};
    wifi_config_t ap_cfg = {0};
    if (sta) {
        copy_str((char *)sta_cfg.sta.ssid, sizeof(sta_cfg.sta.ssid), g_cfg.sta_ssid);
        copy_str((char *)sta_cfg.sta.password, sizeof(sta_cfg.sta.password), g_cfg.sta_pass);
    }
    if (ap) {
        copy_str((char *)ap_cfg.ap.ssid, sizeof(ap_cfg.ap.ssid), g_cfg.ap_ssid);
        copy_str((char *)ap_cfg.ap.password, sizeof(ap_cfg.ap.password), g_cfg.ap_pass);
        ap_cfg.ap.ssid_len = strlen(g_cfg.ap_ssid);
        ap_cfg.ap.max_connection = 4;
        ap_cfg.ap.channel = g_cfg.espnow_enabled ? g_cfg.espnow_channel : 1;
        ap_cfg.ap.authmode = strlen(g_cfg.ap_pass) >= 8 ? WIFI_AUTH_WPA_WPA2_PSK : WIFI_AUTH_OPEN;
    }
    wifi_mode_t mode = sta && ap ? WIFI_MODE_APSTA : ((sta || radio_only) ? WIFI_MODE_STA : WIFI_MODE_AP);
    ESP_ERROR_CHECK(esp_wifi_set_mode(mode));
    if (sta) ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta_cfg));
    if (ap) ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_cfg));
    ESP_ERROR_CHECK(esp_wifi_start());
    if (sta) esp_wifi_connect();
    if (ap && !sta && !g_eth_online) copy_str(g_active_ip, sizeof(g_active_ip), "192.168.4.1");
    start_espnow(ap && !sta);
}

static void start_eth(void)
{
    eth_mac_config_t mac_config = ETH_MAC_DEFAULT_CONFIG();
    eth_phy_config_t phy_config = ETH_PHY_DEFAULT_CONFIG();
    phy_config.phy_addr = ETH_PHY_ADDR;
    phy_config.reset_gpio_num = ETH_PHY_RST_GPIO;
    eth_esp32_emac_config_t emac_config = ETH_ESP32_EMAC_DEFAULT_CONFIG();
    emac_config.smi_gpio.mdc_num = ETH_MDC_GPIO;
    emac_config.smi_gpio.mdio_num = ETH_MDIO_GPIO;
    emac_config.interface = EMAC_DATA_INTERFACE_RMII;
    emac_config.clock_config.rmii.clock_mode = EMAC_CLK_EXT_IN;
    emac_config.clock_config.rmii.clock_gpio = ETH_RMII_CLK_GPIO;
    esp_eth_mac_t *mac = esp_eth_mac_new_esp32(&emac_config, &mac_config);
    esp_eth_phy_t *phy = esp_eth_phy_new_generic(&phy_config);
    esp_eth_config_t config = ETH_DEFAULT_CONFIG(mac, phy);
    esp_eth_handle_t eth_handle = NULL;
    ESP_ERROR_CHECK(esp_eth_driver_install(&config, &eth_handle));
    esp_netif_config_t netif_cfg = ESP_NETIF_DEFAULT_ETH();
    esp_netif_t *eth_netif = esp_netif_new(&netif_cfg);
    apply_static_ip(eth_netif);
    ESP_ERROR_CHECK(esp_netif_attach(eth_netif, esp_eth_new_netif_glue(eth_handle)));
    ESP_ERROR_CHECK(esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID, eth_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_ETH_GOT_IP, ip_event_handler, NULL));
    ESP_ERROR_CHECK(esp_eth_start(eth_handle));
}

static void mqtt_event_handler(void *args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = event_data;
    if (event_id == MQTT_EVENT_CONNECTED) {
        g_mqtt_online = true;
        char topic[96];
        snprintf(topic, sizeof(topic), "%s/status", g_cfg.base_topic);
        esp_mqtt_client_publish(event->client, topic, "online", 0, 0, 1);
        if (!g_mqtt_local) {
            for (int i = 0; i < MAX_GPIO_RULES; i++) {
                if (g_cfg.rules[i].enabled && g_cfg.rules[i].topic[0]) {
                    esp_mqtt_client_subscribe(event->client, g_cfg.rules[i].topic, 0);
                }
            }
            for (int i = 0; i < MAX_MODBUS_RULES; i++) {
                modbus_rule_t *rule = &g_cfg.modbus[i];
                if (!rule->enabled || !rule->topic[0]) continue;
                if (rule->direction == MB_TOPIC_TO_REG || rule->direction == MB_BOTH) {
                    esp_mqtt_client_subscribe(event->client, rule->topic, 0);
                }
            }
            if (g_cfg.espnow_enabled && g_cfg.espnow_tx_topic[0]) {
                esp_mqtt_client_subscribe(event->client, g_cfg.espnow_tx_topic, 0);
            }
        }
    } else if (event_id == MQTT_EVENT_DISCONNECTED) {
        g_mqtt_online = false;
    } else if (event_id == MQTT_EVENT_DATA) {
        char topic[128] = {0};
        int n = event->topic_len < (int)sizeof(topic) - 1 ? event->topic_len : (int)sizeof(topic) - 1;
        memcpy(topic, event->topic, n);
        handle_topic_message(topic, event->data, event->data_len, true);
        handle_modbus_topic_message(topic, event->data, event->data_len);
        handle_espnow_topic_message(topic, event->data, event->data_len);
    }
}

static void start_mqtt_client(void)
{
    if (g_mqtt) return;
    g_mqtt_local = false;
    char uri[96];
    snprintf(uri, sizeof(uri), "mqtt://%s:%d", g_cfg.mqtt_host, g_cfg.mqtt_port);
    char will_topic[96];
    snprintf(will_topic, sizeof(will_topic), "%s/status", g_cfg.base_topic);
    esp_mqtt_client_config_t cfg = {
        .broker.address.uri = uri,
        .credentials.username = g_cfg.mqtt_user[0] ? g_cfg.mqtt_user : NULL,
        .credentials.authentication.password = g_cfg.mqtt_pass[0] ? g_cfg.mqtt_pass : NULL,
        .session.last_will.topic = will_topic,
        .session.last_will.msg = "offline",
        .session.last_will.qos = 0,
        .session.last_will.retain = true,
    };
    g_mqtt = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(g_mqtt, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(g_mqtt);
}

static void start_mqtt_local_client(void)
{
    if (g_mqtt) return;
    g_mqtt_local = true;
    char uri[96];
    snprintf(uri, sizeof(uri), "mqtt://127.0.0.1:%d", g_cfg.broker_port);
    char will_topic[96];
    snprintf(will_topic, sizeof(will_topic), "%s/status", g_cfg.base_topic);
    esp_mqtt_client_config_t cfg = {
        .broker.address.uri = uri,
        .credentials.client_id = MQTT_LOCAL_CLIENT_ID,
        .credentials.username = g_cfg.broker_user[0] ? g_cfg.broker_user : NULL,
        .credentials.authentication.password = g_cfg.broker_pass[0] ? g_cfg.broker_pass : NULL,
        .session.last_will.topic = will_topic,
        .session.last_will.msg = "offline",
        .session.last_will.qos = 0,
        .session.last_will.retain = true,
    };
    g_mqtt = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(g_mqtt, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(g_mqtt);
}

static int broker_auth_cb(const char *client_id, const char *username, const char *password, int password_len)
{
    ESP_LOGI(TAG, "Broker connect: %s", client_id ? client_id : "(none)");
    if (!g_cfg.broker_user[0]) return 0;
    if (!username || strcmp(username, g_cfg.broker_user) != 0) return 1;
    if (!password || password_len != (int)strlen(g_cfg.broker_pass)) return 1;
    return strncmp(password, g_cfg.broker_pass, password_len) == 0 ? 0 : 1;
}

static void broker_message_cb(char *client, char *topic, char *data, int len, int qos, int retain)
{
    ESP_LOGI(TAG, "Broker msg client=%s topic=%s len=%d", client ? client : "", topic ? topic : "", len);
    if (topic && data) {
        log_mqtt_message(client, topic, data, len, qos, retain);
        if (!client || strcmp(client, MQTT_LOCAL_CLIENT_ID) != 0) {
            handle_topic_message(topic, data, len, true);
            handle_modbus_topic_message(topic, data, len);
            handle_espnow_topic_message(topic, data, len);
        }
    }
}

static void broker_task(void *arg)
{
    g_broker_running = true;
    struct mosq_broker_config cfg = {
        .host = "0.0.0.0",
        .port = g_cfg.broker_port,
        .tls_cfg = NULL,
        .handle_message_cb = broker_message_cb,
        .handle_connect_cb = broker_auth_cb,
    };
    ESP_LOGI(TAG, "Starting Espressif Mosquitto broker on :%d", g_cfg.broker_port);
    int rc = mosq_broker_run(&cfg);
    ESP_LOGW(TAG, "Mosquitto broker stopped rc=%d", rc);
    g_broker_running = false;
    g_broker_task = NULL;
    vTaskDelete(NULL);
}

static void modbus_task(void *arg)
{
    (void)arg;
    while (true) {
        uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
        for (int i = 0; i < MAX_MODBUS_RULES; i++) {
            modbus_rule_t *rule = &g_cfg.modbus[i];
            if (!rule->enabled || !rule->topic[0]) continue;
            if (rule->direction != MB_REG_TO_TOPIC && rule->direction != MB_BOTH) continue;
            if ((int32_t)(now - rule->next_poll_ms) < 0) continue;

            int poll_ms = rule->poll_ms < 200 ? 200 : rule->poll_ms;
            rule->next_poll_ms = now + poll_ms;

            uint16_t value = 0;
            if (!modbus_read_holding(rule, &value)) continue;

            bool changed = !rule->has_value || rule->last_value != value;
            rule->last_value = value;
            rule->has_value = true;
            if (changed) {
                char payload[12];
                snprintf(payload, sizeof(payload), "%u", value);
                mqtt_publish_text(rule->topic, payload, false);
            }
        }
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

static void sensor_task(void *arg)
{
    (void)arg;
    while (true) {
        uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
        for (int i = 0; i < MAX_SENSOR_RULES; i++) {
            sensor_rule_t *rule = &g_cfg.sensors[i];
            if (!rule->enabled || !rule->topic[0]) continue;
            if ((int32_t)(now - rule->next_poll_ms) < 0) continue;

            int poll_ms = rule->poll_ms < 1000 ? 1000 : rule->poll_ms;
            rule->next_poll_ms = now + poll_ms;

            float temp = 0, hum = 0, press = 0;
            bool ok = false;
            rule->has_temp = false;
            rule->has_hum = false;
            rule->has_press = false;

            switch (rule->type) {
                case SENSOR_DS18B20:
                    ok = ds18b20_read(rule->pin, rule->rom, rule->resolution, &temp);
                    if (ok) {
                        rule->last_temp = apply_temp_unit(temp + rule->temp_offset, rule->temp_unit);
                        rule->has_temp = true;
                    }
                    break;
                case SENSOR_BME280:
                    ok = bme280_read(rule->sda, rule->scl, rule->addr ? rule->addr : 0x76, rule->resolution, &temp, &hum, &press);
                    if (ok) {
                        rule->last_temp = apply_temp_unit(temp + rule->temp_offset, rule->temp_unit);
                        rule->last_hum = clamp_humidity(hum + rule->hum_offset);
                        rule->last_press = apply_press_unit(press + rule->press_offset, rule->press_unit);
                        rule->has_temp = true;
                        rule->has_hum = true;
                        rule->has_press = true;
                    }
                    break;
                case SENSOR_SHT3X:
                    ok = sht3x_read(rule->sda, rule->scl, rule->addr ? rule->addr : 0x44, rule->resolution, &temp, &hum);
                    if (ok) {
                        rule->last_temp = apply_temp_unit(temp + rule->temp_offset, rule->temp_unit);
                        rule->last_hum = clamp_humidity(hum + rule->hum_offset);
                        rule->has_temp = true;
                        rule->has_hum = true;
                    }
                    break;
                case SENSOR_DHT22:
                case SENSOR_DHT11:
                    ok = dht_read(rule->pin, rule->type == SENSOR_DHT11, &temp, &hum);
                    if (ok) {
                        rule->last_temp = apply_temp_unit(temp + rule->temp_offset, rule->temp_unit);
                        rule->last_hum = clamp_humidity(hum + rule->hum_offset);
                        rule->has_temp = true;
                        rule->has_hum = true;
                    }
                    break;
                default:
                    break;
            }
            if (ok) {
                publish_sensor_reading(rule);
            } else {
                ESP_LOGW(TAG, "Sensor read failed: idx=%d type=%d", i, rule->type);
            }
        }
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

void app_main(void)
{
    g_boot_us = esp_timer_get_time();
    esp_log_level_set("*", ESP_LOG_INFO);
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    }
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    g_mqtt_log_lock = xSemaphoreCreateMutex();
    load_config();
    for (int i = 0; i < MAX_GPIO_RULES; i++) if (g_cfg.rules[i].enabled) apply_gpio_rule(i, false, false);
    bool want_eth = g_cfg.net_mode == NET_ETH || g_cfg.net_mode == NET_ETH_WIFI_AP;
    bool want_sta = g_cfg.net_mode == NET_WIFI_STA;
    bool want_ap = g_cfg.net_mode == NET_WIFI_AP || g_cfg.net_mode == NET_ETH_WIFI_AP;
    bool want_espnow_radio = g_cfg.espnow_enabled && !want_sta && !want_ap;
    if (want_eth) start_eth();
    if (want_sta || want_ap || want_espnow_radio) start_wifi(want_sta, want_ap, want_espnow_radio);
    start_http();
    if (g_cfg.mqtt_mode == MQTT_CLIENT) start_mqtt_client();
    if (g_cfg.mqtt_mode == MQTT_BROKER) {
        xTaskCreate(broker_task, "mosquitto", 8192, NULL, 5, &g_broker_task);
        vTaskDelay(pdMS_TO_TICKS(250));
        start_mqtt_local_client();
    }
    xTaskCreate(modbus_task, "modbus_tcp", 6144, NULL, 4, &g_modbus_task);
    xTaskCreate(sensor_task, "sensors", 8192, NULL, 4, &g_sensor_task);
    ESP_LOGI(TAG, "%s %s ready", FW_NAME, FW_VERSION);
}
