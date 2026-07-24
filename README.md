# Healtcare-Monitoring-System
/* =====================================================================
   REMOTE PATIENT RISK MONITORING SYSTEM
   ESP32 + Wokwi + Adafruit IO
   Covers all 6 internship tasks:
     1. Multi-Sensor Expansion using FreeRTOS (+ watchdog)
     2. Dual-Role IoT Monitoring (Medical / Facility dashboards)
     3. Smart Medication Dosage Adjustment
     4. Intelligent Remote Bed Elevation Control
     5. Smart Dynamic Sampling Rate (Adaptive Monitoring)
     6. Advanced Offline Detection (Fault-Tolerant System)
   ===================================================================== */

#include <WiFi.h>
#include "Adafruit_MQTT.h"
#include "Adafruit_MQTT_Client.h"
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ESP32Servo.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include <esp_task_wdt.h>

// ===================================================================
// PIN DEFINITIONS
// ===================================================================
#define BODY_TEMP_PIN   4
#define ROOM_TEMP_PIN   15
#define SCREEN_WIDTH    128
#define SCREEN_HEIGHT   64
#define OLED_RESET      -1
#define SERVO_PIN       18
#define LED_PIN         19
#define BUZZER_PIN      5
#define AQI_PIN         34
#define OXYGEN_PIN      35
#define BP_PIN          32

// ===================================================================
// WIFI + ADAFRUIT IO
// ===================================================================
#define WIFI_SSID       "Wokwi-GUEST"
#define WIFI_PASS       ""
#define AIO_SERVER      "io.adafruit.com"
#define AIO_SERVERPORT  1883
#define AIO_USERNAME    "bhaktijaiswal_15"
#define AIO_KEY         "aio_KZQX93f16uNqoR8paahitb0n3h5u"

// ===================================================================
// OBJECTS
// ===================================================================
OneWire oneWireBody(BODY_TEMP_PIN);
DallasTemperature bodySensor(&oneWireBody);

OneWire oneWireRoom(ROOM_TEMP_PIN);
DallasTemperature roomSensor(&oneWireRoom);

Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);
Servo bedServo;
WiFiClient client;
Adafruit_MQTT_Client mqtt(&client, AIO_SERVER, AIO_SERVERPORT, AIO_USERNAME, AIO_KEY);

// ===================================================================
// MQTT FEEDS (10 Feeds Total)
// ===================================================================
Adafruit_MQTT_Publish heartRateFeed    = Adafruit_MQTT_Publish(&mqtt, AIO_USERNAME "/feeds/heart-rate");
Adafruit_MQTT_Publish spo2Feed         = Adafruit_MQTT_Publish(&mqtt, AIO_USERNAME "/feeds/spo2");
Adafruit_MQTT_Publish bodyTempFeed     = Adafruit_MQTT_Publish(&mqtt, AIO_USERNAME "/feeds/body-temp");
Adafruit_MQTT_Publish roomTempFeed     = Adafruit_MQTT_Publish(&mqtt, AIO_USERNAME "/feeds/room-temp");
Adafruit_MQTT_Publish oxygenFeed       = Adafruit_MQTT_Publish(&mqtt, AIO_USERNAME "/feeds/oxygen-level");
Adafruit_MQTT_Publish aqiFeed          = Adafruit_MQTT_Publish(&mqtt, AIO_USERNAME "/feeds/aqi");
Adafruit_MQTT_Publish alertsFeed       = Adafruit_MQTT_Publish(&mqtt, AIO_USERNAME "/feeds/alerts");

Adafruit_MQTT_Subscribe dosageSub          = Adafruit_MQTT_Subscribe(&mqtt, AIO_USERNAME "/feeds/dosage");
Adafruit_MQTT_Subscribe bedControlSub      = Adafruit_MQTT_Subscribe(&mqtt, AIO_USERNAME "/feeds/bed-control");
Adafruit_MQTT_Subscribe samplingControlSub = Adafruit_MQTT_Subscribe(&mqtt, AIO_USERNAME "/feeds/sampling-control");

// ===================================================================
// GLOBAL STATE
// ===================================================================
float bodyTemp = 0, roomTemp = 0, bp = 120;
int heartRate = 0, spo2 = 0, oxygenLevel = 0, aqi = 0;

// Task 3: Dosage
int targetDosage      = 0;
int currentDosage      = 0;
int pendingDosage      = 0;
bool awaitingConfirm   = false;
const int DOSAGE_CRITICAL_THRESHOLD = 80;
const int MAX_DOSAGE_STEP_PER_CMD   = 15;
unsigned long lastBlinkTime = 0;
bool ledState = false;
enum DosageLevel { DOSAGE_NORMAL, DOSAGE_WARNING, DOSAGE_CRITICAL };
DosageLevel dosageLevel = DOSAGE_NORMAL;

// Task 4: Bed elevation
int currentBedAngle = 10;
int targetBedAngle  = 10;
volatile int bedMode = 1; // 0=manual, 1=sleeping, 2=breathing, 3=emergency

// Task 5: Adaptive sampling
volatile int samplingRate = 10000;
volatile bool samplingAutoMode = true;
const int SAMPLING_MIN_MS = 5000;
const int SAMPLING_MAX_MS = 60000;

// Task 6: Offline detection
enum SystemState { STATE_ONLINE, STATE_DEGRADED, STATE_OFFLINE };
volatile SystemState systemState = STATE_OFFLINE;
unsigned long lastWifiAttempt = 0;
unsigned long wifiBackoff = 1000;
const unsigned long WIFI_BACKOFF_MAX = 30000;
unsigned long lastMqttAttempt = 0;
unsigned long mqttBackoff = 1000;
const unsigned long MQTT_BACKOFF_MAX = 30000;

// Task 2: Aggregation accumulators
long hrSum = 0, spo2Sum = 0;
int aggSampleCount = 0;
const unsigned long AGG_INTERVAL_MS = 60000;

// ===================================================================
// RTOS PRIMITIVES (Task 1)
// ===================================================================
struct SensorData {
  int heartRate;
  int spo2;
  float bodyTemp;
  float roomTemp;
  int oxygenLevel;
  int aqi;
  float bp;
};

struct BufferedRecord {
  int heartRate;
  int spo2;
  float bodyTemp;
  float roomTemp;
  int oxygenLevel;
  int aqi;
  float bp;
};

#define OFFLINE_BUFFER_SIZE 60
#define ALERT_QUEUE_SIZE 20

struct AlertMsg {
  char text[48];
};

QueueHandle_t sensorQueue;
QueueHandle_t offlineBufferQueue;
QueueHandle_t alertQueue;

SemaphoreHandle_t oledMutex;
SemaphoreHandle_t mqttMutex;
SemaphoreHandle_t dataMutex;

SensorData latestSensorData;

void flushOfflineBufferImpl();

void raiseAlert(const char *text) {
  AlertMsg m;
  strncpy(m.text, text, sizeof(m.text) - 1);
  m.text[sizeof(m.text) - 1] = '\0';
  xQueueSend(alertQueue, &m, 0);
}

int clampInt(int v, int lo, int hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

// ===================================================================
// WIFI & MQTT MANAGERS
// ===================================================================
void beginWifi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
}

void maintainWifi() {
  if (WiFi.status() == WL_CONNECTED) {
    wifiBackoff = 1000;
    return;
  }
  unsigned long now = millis();
  if (now - lastWifiAttempt >= wifiBackoff) {
    lastWifiAttempt = now;
    Serial.println("[WiFi] Reconnecting...");
    WiFi.disconnect();
    WiFi.begin(WIFI_SSID, WIFI_PASS);
    wifiBackoff = min(wifiBackoff * 2, WIFI_BACKOFF_MAX);
  }
}

bool tryMqttConnect() {
  if (mqtt.connected()) return true;
  int8_t ret = mqtt.connect();
  if (ret == 0) {
    Serial.println("[MQTT] Connected!");
    mqtt.subscribe(&dosageSub);
    mqtt.subscribe(&bedControlSub);
    mqtt.subscribe(&samplingControlSub);
    return true;
  } else {
    mqtt.disconnect();
    return false;
  }
}

void wdtSafeDelay(unsigned long totalMs) {
  const unsigned long CHUNK = 2000;
  unsigned long waited = 0;
  while (waited < totalMs) {
    unsigned long slice = min(CHUNK, totalMs - waited);
    vTaskDelay(pdMS_TO_TICKS(slice));
    esp_task_wdt_reset();
    waited += slice;
  }
}

// ===================================================================
// FREERTOS TASKS
// ===================================================================

// Task 6: Connection Monitor
void connectionMonitorTask(void *parameter) {
  esp_task_wdt_add(NULL);
  SystemState previousState = STATE_OFFLINE;
  for (;;) {
    esp_task_wdt_reset();
    maintainWifi();
    bool wifiOk = (WiFi.status() == WL_CONNECTED);
    bool mqttOk = false;
    
    if (wifiOk) {
      if (xSemaphoreTake(mqttMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        mqttOk = mqtt.connected();
        if (!mqttOk) {
          unsigned long now = millis();
          if (now - lastMqttAttempt >= mqttBackoff) {
            lastMqttAttempt = now;
            mqttOk = tryMqttConnect();
            mqttBackoff = mqttOk ? 1000 : min(mqttBackoff * 2, MQTT_BACKOFF_MAX);
          }
        } else {
          mqtt.ping();
        }
        xSemaphoreGive(mqttMutex);
      }
    }

    if (!wifiOk)        systemState = STATE_OFFLINE;
    else if (!mqttOk)   systemState = STATE_DEGRADED;
    else                systemState = STATE_ONLINE;

    if (systemState != previousState) {
      if (xSemaphoreTake(oledMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        display.clearDisplay();
        display.setTextSize(1);
        display.setTextColor(WHITE);
        display.setCursor(0, 20);
        if (systemState == STATE_OFFLINE) {
          display.println("LOGGING OFFLINE");
          display.setCursor(0, 32);
          display.println("WIFI DOWN");
        } else if (systemState == STATE_DEGRADED) {
          display.println("LOGGING OFFLINE");
          display.setCursor(0, 32);
          display.println("MQTT DOWN");
        } else {
          display.println("SYSTEM ONLINE");
        }
        display.display();
        xSemaphoreGive(oledMutex);
      }

      if (systemState == STATE_OFFLINE) {
        tone(BUZZER_PIN, 800); delay(150); noTone(BUZZER_PIN); delay(100);
        tone(BUZZER_PIN, 800); delay(150); noTone(BUZZER_PIN);
      } else if (systemState == STATE_DEGRADED) {
        tone(BUZZER_PIN, 1500); delay(300); noTone(BUZZER_PIN);
      } else if (systemState == STATE_ONLINE && previousState != STATE_ONLINE) {
        tone(BUZZER_PIN, 2000); delay(120); noTone(BUZZER_PIN);
        flushOfflineBufferImpl();
      }

      char msg[32];
      snprintf(msg, sizeof(msg), "STATUS:%s",
               systemState == STATE_ONLINE ? "ONLINE" :
               systemState == STATE_DEGRADED ? "DEGRADED" : "OFFLINE");
      raiseAlert(msg);
      previousState = systemState;
    }
    vTaskDelay(pdMS_TO_TICKS(1000));
  }
}

void bufferReadingIfNeeded(const SensorData &d) {
  if (systemState == STATE_ONLINE) return;
  BufferedRecord rec = { d.heartRate, d.spo2, d.bodyTemp, d.roomTemp, d.oxygenLevel, d.aqi, d.bp };
  if (uxQueueSpacesAvailable(offlineBufferQueue) == 0) {
    BufferedRecord discard;
    xQueueReceive(offlineBufferQueue, &discard, 0);
  }
  xQueueSend(offlineBufferQueue, &rec, 0);
}

// Task 1 & 5: Patient Vitals Sensor Task
void sensorTask(void *parameter) {
  esp_task_wdt_add(NULL);
  while (true) {
    esp_task_wdt_reset();
    bodySensor.requestTemperatures();
    float bTemp = bodySensor.getTempCByIndex(0);
    int hr = random(65, 120);
    int sp = random(90, 100);
    float rTemp, bpV;
    int oxy, aqiV;

    if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
      bodyTemp = bTemp;
      heartRate = hr; spo2 = sp;
      hrSum += hr; spo2Sum += sp; aggSampleCount++;
      rTemp = roomTemp; oxy = oxygenLevel; aqiV = aqi; bpV = bp;
      xSemaphoreGive(dataMutex);
    } else {
      rTemp = roomTemp; oxy = oxygenLevel; aqiV = aqi; bpV = bp;
    }

    SensorData data = { hr, sp, bTemp, rTemp, oxy, aqiV, bpV };
    xQueueSend(sensorQueue, &data, 0);
    bufferReadingIfNeeded(data);

    if (samplingAutoMode) {
      bool abnormal = (hr < 60 || hr > 100 || sp < 92);
      if (abnormal) {
        samplingRate = SAMPLING_MIN_MS;
      } else if (samplingRate < SAMPLING_MAX_MS) {
        samplingRate = min((int)samplingRate + 5000, SAMPLING_MAX_MS);
      }
    } else {
      samplingRate = clampInt(samplingRate, SAMPLING_MIN_MS, SAMPLING_MAX_MS);
    }
    wdtSafeDelay(samplingRate);
  }
}

// Task 1: Environment Task
void environmentTask(void *parameter) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    roomSensor.requestTemperatures();
    float rTemp = roomSensor.getTempCByIndex(0);
    int oxy  = map(analogRead(OXYGEN_PIN), 0, 4095, 0, 100);
    int aqiV = map(analogRead(AQI_PIN), 0, 4095, 0, 500);

    if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
      roomTemp = rTemp; oxygenLevel = oxy; aqi = aqiV;
      xSemaphoreGive(dataMutex);
    }
    wdtSafeDelay(8000);
  }
}

// Task 1: Blood Pressure Task
void vitalsExtraTask(void *parameter) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    float currentBp = map(analogRead(BP_PIN), 0, 4095, 90, 180);
    if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
      bp = currentBp;
      xSemaphoreGive(dataMutex);
    }
    if (currentBp > 160 || currentBp < 95) {
      char msg[48];
      snprintf(msg, sizeof(msg), "BP ABNORMAL: %.0f mmHg", currentBp);
      raiseAlert(msg);
    }
    wdtSafeDelay(4000);
  }
}

// Task 2: Aggregation Task
void aggregationTask(void *parameter) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    wdtSafeDelay(AGG_INTERVAL_MS);
    int hrAvg = 0, spo2Avg = 0;
    if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
      if (aggSampleCount > 0) {
        hrAvg = hrSum / aggSampleCount;
        spo2Avg = spo2Sum / aggSampleCount;
      }
      hrSum = 0; spo2Sum = 0; aggSampleCount = 0;
      xSemaphoreGive(dataMutex);
    }
    char msg[64];
    snprintf(msg, sizeof(msg), "SUMMARY HR_avg:%d SpO2_avg:%d", hrAvg, spo2Avg);
    raiseAlert(msg);
  }
}

// Task 2: MQTT Publish Task
void mqttPublishTask(void *parameter) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    SensorData receivedData;
    if (xQueueReceive(sensorQueue, &receivedData, pdMS_TO_TICKS(100))) {
      latestSensorData = receivedData;
    }

    if (systemState == STATE_ONLINE) {
      if (xSemaphoreTake(mqttMutex, pdMS_TO_TICKS(300)) == pdTRUE) {
        heartRateFeed.publish((int32_t)latestSensorData.heartRate);
        spo2Feed.publish((int32_t)latestSensorData.spo2);
        bodyTempFeed.publish(latestSensorData.bodyTemp);
        roomTempFeed.publish(latestSensorData.roomTemp);
        oxygenFeed.publish((int32_t)latestSensorData.oxygenLevel);
        aqiFeed.publish((int32_t)latestSensorData.aqi);
        xSemaphoreGive(mqttMutex);
      }
      esp_task_wdt_reset();

      if (latestSensorData.heartRate < 60 || latestSensorData.heartRate > 100 || latestSensorData.spo2 < 92) {
        raiseAlert("PATIENT ALERT: VITALS ABNORMAL");
      }
      if (latestSensorData.aqi > 200 || latestSensorData.oxygenLevel < 19) {
        raiseAlert("FACILITY ALERT: ENVIRONMENT");
      }
    }
    wdtSafeDelay(5000);
  }
}

void flushOfflineBufferImpl() {
  BufferedRecord rec;
  int flushed = 0;
  while (xQueueReceive(offlineBufferQueue, &rec, 0) == pdTRUE) {
    esp_task_wdt_reset();
    unsigned long backoff = 500;
    bool ok = false;
    for (int attempt = 0; attempt < 4 && !ok; attempt++) {
      if (xSemaphoreTake(mqttMutex, pdMS_TO_TICKS(300)) == pdTRUE) {
        ok = heartRateFeed.publish((int32_t)rec.heartRate) &&
             spo2Feed.publish((int32_t)rec.spo2) &&
             bodyTempFeed.publish(rec.bodyTemp) &&
             roomTempFeed.publish(rec.roomTemp) &&
             oxygenFeed.publish((int32_t)rec.oxygenLevel) &&
             aqiFeed.publish((int32_t)rec.aqi);
        xSemaphoreGive(mqttMutex);
      }
      if (!ok) {
        esp_task_wdt_reset();
        vTaskDelay(pdMS_TO_TICKS(backoff));
        esp_task_wdt_reset();
        backoff = min(backoff * 2, (unsigned long)8000);
      }
    }
    if (ok) flushed++;
  }
  if (flushed > 0) {
    Serial.print("[Sync] Flushed buffered readings: ");
    Serial.println(flushed);
  }
}

// Task 3: Medication Dosage Logic
void applyRateLimitedDosage(int requested) {
  requested = clampInt(requested, 0, 100);
  if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    int diff = requested - targetDosage;
    diff = clampInt(diff, -MAX_DOSAGE_STEP_PER_CMD, MAX_DOSAGE_STEP_PER_CMD);
    targetDosage = clampInt(targetDosage + diff, 0, 100);
    xSemaphoreGive(dataMutex);
  }
}

void handleDosageCommand(int requested) {
  requested = clampInt(requested, 0, 100);
  if (requested > DOSAGE_CRITICAL_THRESHOLD) {
    if (awaitingConfirm && requested == pendingDosage) {
      applyRateLimitedDosage(requested);
      awaitingConfirm = false;
      raiseAlert("DOSAGE CONFIRMED & APPLIED");
    } else {
      pendingDosage = requested;
      awaitingConfirm = true;
      raiseAlert("DOSAGE: RESEND SAME VALUE TO CONFIRM");
    }
  } else {
    awaitingConfirm = false;
    applyRateLimitedDosage(requested);
  }
}

void updateDosageSafely() {
  if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    if (currentDosage < targetDosage) currentDosage++;
    else if (currentDosage > targetDosage) currentDosage--;

    if (currentDosage <= 50)      dosageLevel = DOSAGE_NORMAL;
    else if (currentDosage <= DOSAGE_CRITICAL_THRESHOLD) dosageLevel = DOSAGE_WARNING;
    else                          dosageLevel = DOSAGE_CRITICAL;
    
    xSemaphoreGive(dataMutex);
  }
}

void handleDosageAlerts() {
  switch (dosageLevel) {
    case DOSAGE_NORMAL:
      digitalWrite(LED_PIN, LOW);
      noTone(BUZZER_PIN);
      break;
    case DOSAGE_WARNING:
      if (millis() - lastBlinkTime > 500) {
        ledState = !ledState;
        digitalWrite(LED_PIN, ledState);
        lastBlinkTime = millis();
      }
      noTone(BUZZER_PIN);
      break;
    case DOSAGE_CRITICAL:
      if (millis() - lastBlinkTime > 150) {
        ledState = !ledState;
        digitalWrite(LED_PIN, ledState);
        lastBlinkTime = millis();
      }
      tone(BUZZER_PIN, 1000);
      break;
  }
}

void dosageTask(void *parameter) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    updateDosageSafely();
    handleDosageAlerts();
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

// Task 4: Bed Elevation Logic
void applyBedMode() {
  if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    switch (bedMode) {
      case 1: targetBedAngle = 10; break;
      case 2: targetBedAngle = 45; break;
      case 3: targetBedAngle = 90; break;
      default: break;
    }
    xSemaphoreGive(dataMutex);
  }
}

void updateBedAngleSmoothly() {
  if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    if (currentBedAngle < targetBedAngle) {
      currentBedAngle++;
      bedServo.write(currentBedAngle);
    } else if (currentBedAngle > targetBedAngle) {
      currentBedAngle--;
      bedServo.write(currentBedAngle);
    }
    xSemaphoreGive(dataMutex);
  }
}

void bedControlTask(void *parameter) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    applyBedMode();
    if (spo2 > 0 && spo2 < 88) {
      if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        targetBedAngle = 90;
        xSemaphoreGive(dataMutex);
      }
    } else if (spo2 > 0 && spo2 < 92 && bedMode != 3) {
      if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        targetBedAngle = 45;
        xSemaphoreGive(dataMutex);
      }
    }
    updateBedAngleSmoothly();
    vTaskDelay(pdMS_TO_TICKS(50));
  }
}

// Task Subscription Handler
void mqttSubscribeTask(void *parameter) {
  esp_task_wdt_add(NULL);
  Adafruit_MQTT_Subscribe *subscription;
  for (;;) {
    esp_task_wdt_reset();
    if (mqtt.connected() && xSemaphoreTake(mqttMutex, pdMS_TO_TICKS(300)) == pdTRUE) {
      while ((subscription = mqtt.readSubscription(200))) {
        if (subscription == &dosageSub) {
          int requested = atoi((char *)dosageSub.lastread);
          Serial.print("[RX] dosage = ");
          Serial.println(requested);
          handleDosageCommand(requested);
        }
        else if (subscription == &bedControlSub) {
          char *val = (char *)bedControlSub.lastread;
          Serial.print("[RX] bed-control = ");
          Serial.println(val);
          if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            if      (strcasecmp(val, "sleep") == 0)     bedMode = 1;
            else if (strcasecmp(val, "breathing") == 0) bedMode = 2;
            else if (strcasecmp(val, "emergency") == 0) bedMode = 3;
            else if (strcasecmp(val, "manual") == 0)    bedMode = 0;
            else {
              bedMode = 0;
              targetBedAngle = clampInt(atoi(val), 0, 90);
            }
            xSemaphoreGive(dataMutex);
          }
        }
        else if (subscription == &samplingControlSub) {
          char *val = (char *)samplingControlSub.lastread;
          Serial.print("[RX] sampling-control = ");
          Serial.println(val);
          if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            if (strcasecmp(val, "auto") == 0) {
              samplingAutoMode = true;
            } else {
              samplingAutoMode = false;
              int seconds = atoi(val);
              samplingRate = clampInt(seconds * 1000, SAMPLING_MIN_MS, SAMPLING_MAX_MS);
            }
            xSemaphoreGive(dataMutex);
          }
        }
      }
      xSemaphoreGive(mqttMutex);
    }
    vTaskDelay(pdMS_TO_TICKS(200));
  }
}

// Central Alert Task
void alertTask(void *parameter) {
  esp_task_wdt_add(NULL);
  AlertMsg m;
  for (;;) {
    esp_task_wdt_reset();
    if (xQueueReceive(alertQueue, &m, pdMS_TO_TICKS(500)) == pdTRUE) {
      if (mqtt.connected() && xSemaphoreTake(mqttMutex, pdMS_TO_TICKS(300)) == pdTRUE) {
        alertsFeed.publish(m.text);
        xSemaphoreGive(mqttMutex);
      }
    }
  }
}

// OLED Display Task
void updateOLED() {
  float bT, bpV;
  int hR, sp, curD, curA, sampR;
  bool conf;
  DosageLevel dL;

  if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    bT = bodyTemp;
    hR = heartRate;
    sp = spo2;
    bpV = bp;
    curD = currentDosage;
    curA = currentBedAngle;
    sampR = samplingRate;
    conf = awaitingConfirm;
    dL = dosageLevel;
    xSemaphoreGive(dataMutex);
  } else {
    return;
  }

  if (xSemaphoreTake(oledMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    display.clearDisplay();
    display.setTextSize(1);
    display.setTextColor(WHITE);
    display.setCursor(0, 0);
    display.println("PATIENT MONITOR");
    display.setCursor(0, 10);
    display.print("T:"); display.print(bT, 1);
    display.print(" HR:"); display.print(hR);
    display.setCursor(0, 20);
    display.print("SpO2:"); display.print(sp);
    display.print("% BP:"); display.print((int)bpV);
    display.setCursor(0, 30);
    display.print("Dose:"); display.print(curD);
    if (dL == DOSAGE_CRITICAL) display.print(" CRIT");
    else if (conf) display.print(" CONFIRM?");
    display.setCursor(0, 40);
    display.print("Bed:"); display.print(curA);
    display.print(" deg  Samp:"); display.print(sampR / 1000); display.print("s");
    display.setCursor(0, 52);
    display.print("Status: ");
    display.print(systemState == STATE_ONLINE ? "ONLINE" :
                  systemState == STATE_DEGRADED ? "DEGRADED" : "OFFLINE");
    display.display();
    xSemaphoreGive(oledMutex);
  }
}

void oledTask(void *parameter) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    if (systemState == STATE_ONLINE) updateOLED();
    vTaskDelay(pdMS_TO_TICKS(200)); // Refresh OLED frequently (5Hz)
  }
}

// ===================================================================
// SETUP & MAIN LOOP
// ===================================================================
void setup() {
  Serial.begin(115200);
  pinMode(LED_PIN, OUTPUT);
  pinMode(BUZZER_PIN, OUTPUT);

  bodySensor.begin();
  roomSensor.begin();
  Wire.begin(21, 22);
  display.begin(SSD1306_SWITCHCAPVCC, 0x3C);
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(WHITE);
  display.setCursor(0, 20);
  display.println("SYSTEM STARTING");
  display.display();

  bedServo.attach(SERVO_PIN);
  bedServo.write(currentBedAngle);

  beginWifi();

  sensorQueue = xQueueCreate(10, sizeof(SensorData));
  offlineBufferQueue = xQueueCreate(OFFLINE_BUFFER_SIZE, sizeof(BufferedRecord));
  alertQueue = xQueueCreate(ALERT_QUEUE_SIZE, sizeof(AlertMsg));

  oledMutex  = xSemaphoreCreateMutex();
  mqttMutex  = xSemaphoreCreateMutex();
  dataMutex  = xSemaphoreCreateMutex();

  esp_task_wdt_config_t twdtConfig = {
    .timeout_ms = 20000,
    .idle_core_mask = (1 << portNUM_PROCESSORS) - 1,
    .trigger_panic = true
  };
  esp_task_wdt_init(&twdtConfig);

  xTaskCreate(sensorTask,            "Sensor",       4000, NULL, 2, NULL);
  xTaskCreate(environmentTask,       "Environment",  3000, NULL, 1, NULL);
  xTaskCreate(vitalsExtraTask,       "ExtraVitals",  3000, NULL, 1, NULL);
  xTaskCreate(aggregationTask,       "Aggregation",  3000, NULL, 1, NULL);
  xTaskCreate(mqttPublishTask,       "MQTTPublish",  5000, NULL, 2, NULL);
  xTaskCreate(mqttSubscribeTask,     "MQTTSubscribe",5000, NULL, 2, NULL);
  xTaskCreate(connectionMonitorTask, "ConnMonitor",  4000, NULL, 3, NULL);
  xTaskCreate(alertTask,             "Alerts",       3000, NULL, 2, NULL);
  xTaskCreate(dosageTask,            "Dosage",       3000, NULL, 1, NULL);
  xTaskCreate(bedControlTask,        "BedControl",   3000, NULL, 1, NULL);
  xTaskCreate(oledTask,              "OLED",         3000, NULL, 1, NULL);
}

void loop() {
  // All work managed by FreeRTOS tasks
}
