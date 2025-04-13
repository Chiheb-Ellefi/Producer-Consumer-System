# 📨 Asynchronous Messaging System (Inspired by Kafka)

This project is a simplified, non-distributed **asynchronous messaging system** modeled after Kafka. Developed in Java as part of a Systems Programming course, it demonstrates key concepts of concurrent programming and synchronization through a basic **publisher-subscriber** model using socket communication and file-based persistence.

## 📌 Features

- Publisher/Subscriber architecture
- Java socket-based communication
- Persistent storage using log and index files
- Binary data storage with `RandomAccessFile`
- Topic-based messaging system
- Efficient read using file indexing
- Thread-safe synchronization using `ReentrantLock`

---

## 🧱 Architecture

The system consists of three main components:

- **Broker**: Receives messages from producers, stores them in topic-specific log files, and handles message delivery to consumers.
- **Producer**: Sends messages to a specified topic. If the topic doesn't exist, the broker creates it.
- **Consumer**: Requests messages from the broker starting at a specific offset.

Communication is managed using **Java sockets**, and each topic is stored in a pair of files:

- `.dat` log file: Stores messages along with their offset and size.
- `.dat` index file: Stores metadata for quick access to every 100th message.

---

## 🔧 Implementation Details

### LogEntry

Manages the writing and reading of messages.

- `appendMessage`: Writes the offset, size, and content of the message to the log.
- `readMessages`: Uses the index file to read messages from a specific offset.

### IndexEntry

Manages the log indexing.

- `findPositionInIndex`: Binary search to find the correct log offset.
- `updateIndex`: Updates the index when new messages are added.
- `getNextOffset`: Retrieves the next available offset in the log file.

---

## 🔐 Synchronization Strategy

To ensure data consistency in concurrent environments, three versions of the system were implemented:

1. **No Synchronization**: Fastest but unsafe—leads to inconsistent offsets and corrupted logs.
2. **Semaphore-Based**: Thread-safe but blocks both reads and writes—affects performance.
3. **ReentrantLock (Final Version)**: Differentiates read/write access:
   - Concurrent reads allowed
   - Exclusive write access ensures performance and integrity

---

## 📊 Performance Insights

- 🧪 **Without Sync**: High throughput, but unstable under concurrency.
- 🧪 **Semaphore Sync**: Data integrity ensured, but low scalability.
- 🧪 **ReentrantLock**: Balanced approach with optimal read performance and data safety.


---

## 🎓 Project Details

- 🧑‍💻 Author: **Chiheb Ellefi**
- 📅 Presented: **March 12, 2025**
- 🏫 Institution: **Institut Supérieur d’Informatique, Université de Tunis El Manar**
- 🎓 Course: **Systèmes d’Exploitation (Operating Systems)**

---

## 📜 License

This project is part of an academic assignment and is open for educational use.
