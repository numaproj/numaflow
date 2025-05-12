# Logs View

The **Logs View** allows users to inspect the logs of a specific container within a pod of a vertex. This guide will walk you through the Logs tab and its various features.

---

## Navigating to the Logs Tab

1. **Select a Pod**
   Navigate to the `Pods View` tab after selecting the vertex and select a pod by name from the `Select a Pod by Name` dropdown.
   ![Select Pod](../../assets/logs/select-pod.png)

2. **Select a Container**
   Choose a container from the `Select a Container` section.
   ![Select Container](../../assets/logs/select-container.png)

3. **View Logs**
   Open the **Logs Tab** on the right to view the container logs.
   ![Logs View](../../assets/logs/logs-tab.png)

---

## Features

### 1. Previous Terminated Container Logs

- Enable the checkbox to view logs from previously terminated containers.
- This is particularly useful for debugging issues.

---

### 2. Search Logs

- Filter logs by typing keywords in the **Search Logs** box.
  ![Search Logs](../../assets/logs/logs-search.png)

---

### 3. Negate Search

- Enable the **Negate Search** option to exclude logs matching the search keywords from the view.
  ![Negate Search](../../assets/logs/logs-negate-search.png)

---

### 4. Wrap Lines

- Use the **Wrap Lines** feature to avoid horizontal scrolling for long log lines, improving readability.
  ![Wrap Lines](../../assets/logs/logs-wrap.png)

---

### 5. Pause Logs

- Pause the log stream to inspect logs when there is a high volume of data.
  ![Pause Logs](../../assets/logs/logs-pause.png)

---

### 6. Dark Mode

- Toggle between **Dark Mode** and **Light Mode** for better visibility based on your preference.
  ![Dark Mode](../../assets/logs/logs-dark-mode.png)

---

### 7. Ascending/Descending Order

- Switch between ascending and descending order of log timestamps for easier navigation.
  ![Ascending Order](../../assets/logs/logs-ascending.png)

---

### 8. Download Logs

- Download the last 1000 logs for offline analysis.

---

### 9. Add/Remove Timestamps

- Toggle timestamps in the logs based on your requirements.
  ![Add Timestamps](../../assets/logs/logs-add-timestamps.png)

---

### 10. Level-Based Filtering

- Filter logs by log levels such as:

  - **Info**
  - **Error**
  - **Warn**
  - **Debug**

  ![Error Filter](../../assets/logs/logs-filter-error.png)

---
