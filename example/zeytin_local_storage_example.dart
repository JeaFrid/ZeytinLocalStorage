import 'dart:async';
import 'package:zeytin_local_storage/zeytin_local_storage.dart';

Future<void> main() async {
  // 1. Initialize Zeytin Storage
  final zeytin = ZeytinStorage(
    namespace: "cli_namespace",
    truckID: "cli_truck",
    encrypter: ZeytinCipher(
      "my_super_secret_password_123456",
    ), // Padded to 32 chars
  );

  print("[*] Initializing Zeytin Engine...");
  await zeytin.initialize("./zeytin");
  await ZeytinMini.init("./zeytin");
  print("[+] Initialization complete.\n");

  final boxName = "cli_users_box";

  // --- 2. Add Normal Data ---
  print("[*] Adding normal data...");
  await _addNormalData(zeytin, boxName);

  // --- 3. Add Encrypted Data ---
  print("[*] Adding encrypted data...");
  await _addEncryptedData(zeytin, boxName);

  // --- 4. Add Batch Data ---
  print("[*] Adding batch data...");
  await _addBatchData(zeytin, boxName);

  // --- 5. Read Single Data ---
  print("\n[*] Reading single data (tag: user_1)...");
  await _readSingleData(zeytin, boxName, "user_1");

  // --- 6. Read Encrypted Data ---
  print("\n[*] Reading encrypted data (tag: secret_user)...");
  await _readSingleData(zeytin, boxName, "secret_user");

  // --- 7. Search Data ---
  print("\n[*] Searching for users with names starting with 'Da'...");
  await _searchData(zeytin, boxName, "name", "Da");

  // --- 8. Filter Data ---
  print("\n[*] Filtering for users older than 25...");
  await _filterData(zeytin, boxName);

  // --- 9. Test ZeytinMini ---
  print("\n[*] Testing ZeytinMini (Auto-Encrypted)...");
  await _testZeytinMini();

  // --- 10. Clean Up ---
  print("\n[*] Cleaning up database...");
  await _cleanup(zeytin);
}

// --- Helper Functions to keep main() clean ---

Future<void> _addNormalData(ZeytinStorage zeytin, String boxId) async {
  final completer = Completer<void>();
  await zeytin.add(
    data: ZeytinValue(boxId, "user_1", {"name": "Alice", "age": 30}),
    onSuccess: () {
      print("  -> Added 'Alice' successfully.");
      completer.complete();
    },
    onError: (e, s) {
      print("  -> Error: $e");
      completer.completeError(e);
    },
  );
  return completer.future;
}

Future<void> _addEncryptedData(ZeytinStorage zeytin, String boxId) async {
  final completer = Completer<void>();
  await zeytin.add(
    data: ZeytinValue(boxId, "secret_user", {
      "name": "Agent 47",
      "mission": "Classified",
    }),
    isEncrypt: true,
    onSuccess: () {
      print("  -> Added encrypted 'Agent 47' successfully.");
      completer.complete();
    },
    onError: (e, s) {
      print("  -> Error: $e");
      completer.completeError(e);
    },
  );
  return completer.future;
}

Future<void> _addBatchData(ZeytinStorage zeytin, String boxId) async {
  final completer = Completer<void>();
  final users = [
    ZeytinValue(boxId, "user_batch_1", {"name": "Bob", "age": 22}),
    ZeytinValue(boxId, "user_batch_2", {"name": "Charlie", "age": 28}),
    ZeytinValue(boxId, "user_batch_3", {"name": "David", "age": 35}),
  ];

  await zeytin.addBatch(
    boxId: boxId,
    entries: users,
    onSuccess: () {
      print("  -> Batch added 3 users successfully.");
      completer.complete();
    },
    onError: (e, s) {
      print("  -> Error: $e");
      completer.completeError(e);
    },
  );
  return completer.future;
}

Future<void> _readSingleData(
  ZeytinStorage zeytin,
  String boxId,
  String tag,
) async {
  final completer = Completer<void>();
  await zeytin.get(
    boxId: boxId,
    tag: tag,
    onSuccess: (result) {
      if (result.value != null) {
        print("  -> Found: ${result.value}");
      } else {
        print("  -> Data not found.");
      }
      completer.complete();
    },
    onError: (e, s) {
      print("  -> Error: $e");
      completer.completeError(e);
    },
  );
  return completer.future;
}

Future<void> _searchData(
  ZeytinStorage zeytin,
  String boxId,
  String field,
  String prefix,
) async {
  final completer = Completer<void>();
  await zeytin.search(
    boxId: boxId,
    field: field,
    prefix: prefix,
    onSuccess: (results) {
      print("  -> Found ${results.length} result(s):");
      for (var res in results) {
        print("     - ${res.tag}: ${res.value}");
      }
      completer.complete();
    },
    onError: (e, s) {
      print("  -> Error: $e");
      completer.completeError(e);
    },
  );
  return completer.future;
}

Future<void> _filterData(ZeytinStorage zeytin, String boxId) async {
  final completer = Completer<void>();
  await zeytin.filter(
    boxId: boxId,
    predicate: (data) => data['age'] != null && data['age'] > 25,
    onSuccess: (results) {
      print("  -> Found ${results.length} result(s) older than 25:");
      for (var res in results) {
        print("     - ${res.tag}: ${res.value}");
      }
      completer.complete();
    },
    onError: (e, s) {
      print("  -> Error: $e");
      completer.completeError(e);
    },
  );
  return completer.future;
}

Future<void> _testZeytinMini() async {
  await ZeytinMini.add("server_config", {"host": "127.0.0.1", "port": 8080});
  print("  -> Added 'server_config' via ZeytinMini.");

  final data = await ZeytinMini.get("server_config");
  print("  -> Read 'server_config': $data");

  await ZeytinMini.remove("server_config");
  print("  -> Removed 'server_config'.");
}

Future<void> _cleanup(ZeytinStorage zeytin) async {
  final completer = Completer<void>();
  await zeytin.deleteAll(
    onSuccess: () {
      print("  -> All data completely wiped.");
      completer.complete();
    },
    onError: (e, s) {
      print("  -> Error: $e");
      completer.completeError(e);
    },
  );
  return completer.future;
}
