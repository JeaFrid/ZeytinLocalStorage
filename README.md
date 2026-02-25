# Zeytin🫒 Local Storage

Zeytin is a database solution developed with care and love for server and local solutions. ZeytinEngine is used for both the server side and the local side. ZeytinEngine is Zeytin's database engine.

## What's Inside?

1. [Create Zeytin](#create-zeytin)
2. [Use the ZeytinValue Data Type](#use-the-zeytinvalue-data-type)
3. [Database Operations](#database-operations)
   - [Adding Data](#adding-data)
     - [Adding Encrypted Data](#adding-encrypted-data)
     - [Adding Auto-Deleting Data (TTL)](#adding-auto-deleting-data-ttl)
   - [Adding Bulk Data (Batch)](#adding-bulk-data-batch)
4. [Data Reading Operations](#data-reading-operations)
   - [Reading Single Data](#reading-single-data)
   - [Reading All Data in the Box](#reading-all-data-in-the-box)
5. [Search and Filtering](#search-and-filtering)
   - [Text-Based Search (Prefix)](#text-based-search-prefix)
   - [Advanced Filtering (Predicate)](#advanced-filtering-predicate)
6. [Listening to Live Data (Stream / Reactive)](#listening-to-live-data-stream--reactive)
   - [Listening to a Single Data](#listening-to-a-single-data)
   - [Listening to the Entire Box](#listening-to-the-entire-box)
7. [Data Deletion and Management Operations](#data-deletion-and-management-operations)
   - [Deletion Operations](#deletion-operations)
   - [Existence Checks (Exists)](#existence-checks-exists)
   - [Maintenance and Closing](#maintenance-and-closing)
8. [Use ZeytinMini](#use-zeytinmini)
   - [Starting (Initialization)](#starting-initialization)
   - [Adding and Reading Data](#adding-and-reading-data)
   - [Fetching Bulk Data](#fetching-bulk-data)
   - [Control and Deletion Operations](#control-and-deletion-operations)

## Create Zeytin

```dart
// Create Zeytin's global variable. It will be used to access the database throughout the application.

ZeytinStorage zeytin = ZeytinStorage(
    namespace: "test",
    truckID: "test_truck"
  );

void main() {
  // You must provide a directory path where Zeytin will store its files.
  // For Pure Dart/Server, provide a local path (e.g., "./zeytin_db").
  // For Flutter, you can use the path_provider package to get a valid device path.
  String basePath = "./zeytin_db";

  await zeytin.initialize(basePath);

  // Your application starts here...
}
```

## Use the ZeytinValue Data Type

ZeytinValue, in short, is the incoming-outgoing data itself.

- box: Box content. The cluster where data is gathered together.
- tag: The key to access the data.
- value`<Map<String, dynamic>>`: The data.

## Database Operations

In this section, you will learn all the basic operations such as adding data and fetching data. Zeytin uses the `onSuccess` and `onError` callback logic instead of direct return values (return) to manage asynchronous operations.

### Adding Data

```dart
// This operation works asynchronously. Waiting is mandatory because there is file-to-file interaction.

await zeytin.add(
  data: ZeytinValue("box", "tag", {"value": "John"}),
  onSuccess: () => print("Data added successfully!"),
  onError: (e, s) => print("An error occurred: $e"),
);
```

#### Adding Encrypted Data

To be able to add encrypted data, you need to set a password and you must keep this password very well. The password must be specified at the beginning.

```dart
// Create Zeytin's global variable. It will be used to access the database throughout the application.

ZeytinStorage zeytin = ZeytinStorage(
    namespace: "test",
    truckID: "test_truck",
    encrypter: ZeytinCipher("password"), // Password
  );

// ZeytinCipher is the encrypter. It uses this;
// `AES(_key, mode: encrypt.AESMode.cbc, padding: 'PKCS7')`

void main() {
  String basePath = "./zeytin_secure_db";
  await zeytin.initialize(basePath);

  // Your application starts here...
}
```

While adding data with encryption support;

```dart
await zeytin.add(
  data: ZeytinValue("box", "tag", {"value": "John"}),
  isEncrypt: true, // Encrypted.
  onSuccess: () => print("Encrypted data added!"),
);
```

Encrypted data does not cause problems in features such as search operations between data. All functions are designed to decrypt these ciphers in the background.

#### Adding Auto-Deleting Data (TTL)

Auto-deleting data works with a passive deletion process. After a certain time, the data is killed on the database.

```dart
await zeytin.add(
  data: ZeytinValue("box", "tag", {"value": "John"}),
  ttl: Duration(days: 1),
  onSuccess: () => print("Temporary data added!"),
);
```

This data will disappear after 1 day. While performing caching operations, this method can be used for the accuracy of the data.

### Adding Bulk Data (Batch)

Adding multiple data to the same box at once is much more efficient in terms of performance.

```dart
List<ZeytinValue> users = [
  ZeytinValue("users_box", "user_1", {"name": "Ali", "age": 25}),
  ZeytinValue("users_box", "user_2", {"name": "Ayşe", "age": 28}),
];

await zeytin.addBatch(
  boxId: "users_box",
  entries: users,
  isEncrypt: false,
  onSuccess: () => print("Users added successfully!"),
  onError: (e, s) => print("Error occurred: $e"),
);
```

---

## Data Reading Operations

When trying to read expired (TTL) data, Zeytin automatically cleans this data and returns `null` to you. Encrypted data is automatically decrypted.

### Reading Single Data

`get` is used to fetch the data with a specific tag in a specific box.

```dart
await zeytin.get(
  boxId: "users_box",
  tag: "user_1",
  onSuccess: (ZeytinValue result) {
    if (result.value != null) {
      print("User Name: ${result.value!['name']}");
    } else {
      print("Data not found or expired.");
    }
  },
);
```

### Reading All Data in the Box

`getBox` is used to fetch all records in a box as a list.

```dart
await zeytin.getBox(
  boxId: "users_box",
  onSuccess: (List<ZeytinValue> result) {
    for (var user in result) {
      print("ID: ${user.tag}, Data: ${user.value}");
    }
  },
);
```

---

## Search and Filtering

### Text-Based Search (Prefix)

It searches inside the box based on whether a specific field **starts with** the text you provided. For example, to find those whose name starts with "Al":

```dart
await zeytin.search(
  boxId: "users_box",
  field: "name",
  prefix: "Al",
  onSuccess: (List<ZeytinValue> results) {
    print("${results.length} results found.");
  },
);
```

### Advanced Filtering (Predicate)

If your search is based on a more complex logic (for example, those older than 25), you can use the `filter` function.

```dart
await zeytin.filter(
  boxId: "users_box",
  predicate: (Map<String, dynamic> data) {
    // Filter those older than 25
    return data['age'] != null && data['age'] > 25;
  },
  onSuccess: (List<ZeytinValue> results) {
    print("${results.length} people are older than 25.");
  },
);
```

---

## Listening to Live Data (Stream / Reactive)

Zeytin allows you to instantly listen (watch) to changes in the database.

### Listening to a Single Data

```dart
// Triggered when there is a change, deletion, or update in the data tagged 'user_1'.
Stream<ZeytinValue> userStream = zeytin.watch("users_box", "user_1");

userStream.listen((ZeytinValue data) {
  print("User data changed: ${data.value}");
});
```

### Listening to the Entire Box

```dart
// Triggered when data is added to, deleted from, or updated in the box.
Stream<List<ZeytinValue>> boxStream = zeytin.watchBox("users_box");

boxStream.listen((List<ZeytinValue> boxData) {
  print("Current number of data in the box: ${boxData.length}");
});
```

---

## Data Deletion and Management Operations

### Deletion Operations

```dart
// 1. Deleting a specific data
await zeytin.remove(
  boxId: "users_box",
  tag: "user_1",
  onSuccess: () => print("User deleted."),
);

// 2. Deleting a box (and all data inside it)
await zeytin.removeBox(boxId: "users_box");

// 3. Completely deleting the current Truck (database file)
await zeytin.removeTruck();

// 4. Cleaning the entire system (Deletes everything)
await zeytin.deleteAll();
```

### Existence Checks (Exists)

You can quickly check whether a data or box exists without reading the data.

```dart
await zeytin.existsTag(
  boxId: "users_box",
  tag: "user_1",
  onSuccess: (bool exists) {
    if (exists) print("Record exists!");
  }
);

// Similarly:
// zeytin.existsBox(...)
// zeytin.existsTruck(...)
```

### Maintenance and Closing

When too many delete/update operations are performed on the database, fragmentation may occur on the disk. The `compact` function optimizes the file by compressing it.

```dart
// Optimize the database file (Cleans unnecessary spaces)
await zeytin.compact(
  onSuccess: () => print("Database optimized!"),
);

// Securely terminate the connection when the application closes
await zeytin.close(
  onSuccess: () => print("Zeytin closed successfully."),
);
```

## Use ZeytinMini

If you prefer the traditional `async/await` (Future) structure over the callback (`onSuccess`, `onError`) architecture and want to perform operations quickly over a single box in your application, you can use the `ZeytinMini` class.

This class converts Zeytin's powerful asynchronous architecture into a simple Key-Value store by using `Completer` in the background. Moreover, all data is **automatically encrypted** in the background.

### Starting (Initialization)

At the beginning of your application, you need to initialize `ZeytinMini` before proceeding to database operations. This process prepares the database and encryption modules. You must provide a directory path.

```dart
String basePath = "./zeytin_mini_db";
await ZeytinMini.init(basePath);
```

> **Note:** The `init` method includes a short safe waiting time of 300 milliseconds in the background to ensure that the system and encryption modules are fully ready.

### Adding and Reading Data

`ZeytinMini` always uses encryption in the background while saving data. When reading data, it automatically decrypts this cipher for you.

```dart
// Adding data (Automatically encrypted in the background)
await ZeytinMini.add("user_token", {"token": "abc123xyz", "expiresIn": 3600});

// Reading data (Cipher automatically decrypted)
Map<String, dynamic>? userData = await ZeytinMini.get("user_token");

if (userData != null) {
  print("Token: ${userData['token']}");
} else {
  print("Record not found.");
}
```

### Fetching Bulk Data

You can directly get all keys (key/tag) or all data in the box as a list. It is very useful especially when creating listing interfaces.

```dart
// List all registered keys
List<String> keys = await ZeytinMini.getAllKeys();
print("Registered Keys: $keys");

// List all registered data
List<Map<String, dynamic>> values = await ZeytinMini.getAllValues();
print("All Data: $values");
```

### Control and Deletion Operations

Checking whether a specific key is in the system or cleaning the data is quite simple.

```dart
// Data existence check
bool hasToken = await ZeytinMini.contains("user_token");

if (hasToken) {
  // Deleting single data
  await ZeytinMini.remove("user_token");
  print("Token deleted.");
}

// Resetting the system (Completely deletes everything in the Mini box)
await ZeytinMini.clear();
```

# ZeytinLocalStorage
