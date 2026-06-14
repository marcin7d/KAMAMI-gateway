# KAMAMI Universal Gateway

Kompleksowa bramka ESP-IDF dla plytki KAmod ESP32 ETH+PoE. Projekt laczy lokalny broker MQTT, klienta MQTT, panel WWW, sterowanie GPIO, Modbus TCP, czujniki smarthome oraz ESP-NOW przy pracy po Ethernet.

## Najwazniejsze funkcje

- Panel WWW do konfiguracji runtime bez ciaglego restartowania ESP.
- Tryb MQTT klienta przez `espressif/mqtt`.
- Lokalny broker MQTT przez komponent `espressif/mosquitto`.
- MQTT Explorer pokazujacy wiadomosci przechodzace przez lokalny broker.
- Mapowanie topicow MQTT na wyjscia GPIO.
- Modbus TCP:
  - topic MQTT -> holding register,
  - holding register -> topic MQTT,
  - tryb dwukierunkowy.
- Czujniki smarthome:
  - DS18B20,
  - BME280,
  - SHT3x/SHT30/SHT31,
  - DHT22/AM2302,
  - DHT11.
- Konfiguracja czujnikow:
  - nazwa robocza do kafelkow na stronie glownej,
  - topic MQTT,
  - interwal raportowania,
  - skanowanie adresu I2C albo ROM DS18B20,
  - jednostki temperatury i cisnienia,
  - offsety,
  - rozdzielczosc/oversampling/repeatability.
- ESP-NOW przy pracy przez Ethernet:
  - WiFi jako samo radio ESP-NOW,
  - most MQTT -> ESP-NOW,
  - most ESP-NOW -> MQTT.
- Konfiguracja Ethernet/WiFi/AP/STA.
- Zakladka `System`:
  - eksport konfiguracji JSON,
  - import konfiguracji JSON,
  - aktualizacja firmware OTA przez upload pliku `.bin`.

## Gotowy firmware

Gotowy obraz aplikacji znajduje sie w:

```text
firmware/kamami_gateway.bin
```

Obraz jest budowany dla ESP32 i ukladu partycji OTA z dwoma slotami aplikacji. Do pierwszego flashowania albo po zmianie `partitions.csv` najbezpieczniej uzyc `idf.py flash`, bo wgrywa rowniez bootloader, `otadata` i tablice partycji.

Po jednorazowym wgraniu ukladu OTA kolejne aktualizacje aplikacji mozna wykonywac z panelu WWW w zakladce `System`, wybierajac plik `.bin`.

## Domyslny start

Po pierwszym uruchomieniu firmware startuje domyslnie jako Ethernet + awaryjny AP.

- AP SSID: `KAMAMI-Gateway`
- AP haslo: `kamami1234`
- WWW przez AP: `http://192.168.4.1/`
- WWW przez Ethernet: adres przydzielony z DHCP
- MQTT broker lokalny: port `1883`
- Base topic: `kamami/gateway`

## Build

```powershell
powershell -ExecutionPolicy Bypass -NoProfile -Command ". C:\Espressif\tools\Microsoft.v6.0.1.PowerShell_profile.ps1; idf.py set-target esp32; idf.py build"
```

## Flash

```powershell
powershell -ExecutionPolicy Bypass -NoProfile -Command ". C:\Espressif\tools\Microsoft.v6.0.1.PowerShell_profile.ps1; idf.py -p COM8 flash"
```

## Ethernet

Domyslne piny RMII sa ustawione w `main/main.c`:

- PHY: generic/LAN8720-compatible
- PHY address: `0`
- MDC: `GPIO23`
- MDIO: `GPIO18`
- RMII clock input: `GPIO0`
- PHY reset/power: `-1`

Jesli dana rewizja plytki KAmod ma inne podlaczenie PHY, zmien stale `ETH_*` na gorze pliku.

## Bezpieczne GPIO dla czujnikow

Panel ogranicza wybor GPIO dla czujnikow do konserwatywnej listy:

```text
GPIO13, GPIO14, GPIO16, GPIO17, GPIO32, GPIO33
```

Celowo nie sa proponowane piny zajete lub ryzykowne na ESP32 z Ethernetem:

- RMII/PHY: `GPIO0`, `GPIO18`, `GPIO19`, `GPIO21`, `GPIO22`, `GPIO23`, `GPIO25`, `GPIO26`, `GPIO27`
- flash: `GPIO6`-`GPIO11`
- UART: `GPIO1`, `GPIO3`
- input-only: `GPIO34`-`GPIO39`
- wybrane piny strapujace boot.

## MQTT GPIO

Przyklad domyslnej reguly:

```text
kamami/gateway/gpio/2/set -> GPIO2
```

Akceptowane payloady:

```text
ON, OFF, 1, 0, true, false, high, low
```

Stan wyjscia publikowany jest na:

```text
<base_topic>/gpio/<pin>/state
```

## Modbus TCP

Kazda regula Modbus TCP zawiera:

- kierunek: topic -> rejestr, rejestr -> topic albo oba,
- host i port Modbus TCP,
- unit id,
- numer rejestru holding,
- topic MQTT,
- interwal odczytu dla kierunku rejestr -> topic.

Zapis rejestru uzywa funkcji Modbus `FC6`, a odczyt holding register uzywa `FC3`.

## Czujniki

Kazdy czujnik publikuje JSON na swoim topicu oraz osobne subtopiki:

```text
<topic>
<topic>/temperature
<topic>/humidity
<topic>/pressure
```

Nazwa robocza czujnika sluzy do wyswietlania wartosci na stronie glownej.

## ESP-NOW

ESP-NOW mozna wlaczyc w zakladce `Siec`. Gdy aktywny jest tryb `Ethernet`, WiFi startuje jako samo radio ESP-NOW, bez STA/AP i bez adresu IP.

- `Topic TX ESP-NOW`: publikacja MQTT na ten topic wysyla payload jako ramke ESP-NOW.
- `Topic RX ESP-NOW`: odebrana ramka ESP-NOW jest publikowana jako JSON.

Przykladowy payload RX:

```json
{"src":"AA:BB:CC:DD:EE:FF","len":5,"rssi":-42,"payload":"hello"}
```

## Komponenty Espressif

Projekt korzysta z komponentow:

- `espressif/mosquitto`
- `espressif/mqtt`
- `espressif/cjson`
