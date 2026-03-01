import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:zeytin_local_storage/src/engine.dart';
import 'package:zeytin_local_storage/src/zeytin_cipher.dart';

class ZeytinValue {
  final String box;
  final String tag;
  final Map<String, dynamic>? value;

  ZeytinValue(this.box, this.tag, this.value);

  ZeytinValue copyWith({
    String? box,
    String? tag,
    Map<String, dynamic>? value,
  }) {
    return ZeytinValue(
      box ?? this.box,
      tag ?? this.tag,
      value ?? this.value,
    );
  }

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'box': box,
      'tag': tag,
      'value': value,
    };
  }

  factory ZeytinValue.fromMap(Map<String, dynamic> map) {
    return ZeytinValue(
      map['box'] as String,
      map['tag'] as String,
      map['value'] != null
          ? Map<String, dynamic>.from(map['value'] as Map)
          : null,
    );
  }

  @override
  String toString() => 'ZeytinValue(box: $box, tag: $tag, value: $value)';
}

class ZeytinStorage {
  final String namespace;
  final String truckID;
  Zeytin? handler;
  ZeytinCipher? encrypter = ZeytinCipher("zeytin_password");
  ZeytinStorage({
    required this.namespace,
    required this.truckID,
    this.encrypter,
  });

  Future<void> initialize(String basePath) async {
    try {
      final directory = Directory(basePath);
      if (!await directory.exists()) {
        await directory.create(recursive: true);
      }
      final testFile = File('$basePath/.test');
      await testFile.writeAsString('test');
      await testFile.delete();
      handler = Zeytin(basePath);
    } catch (e) {
      throw Exception('Zeytin initialization failed: $e');
    }
  }

  Future<void> _maybeTryAsync(
    Future<void> Function() fun, {
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    try {
      if (handler == null) {
        throw Exception(
          "Zeytin has not been started. Use the initialize() command to start Zeytin!",
        );
      } else {
        await fun();
        if (onSuccess != null) {
          onSuccess();
        }
      }
    } catch (e, s) {
      if (onError != null) {
        onError(e.toString(), s.toString());
      }
    }
  }

  Future<void> _maybeTryAsyncWithReturn<T>(
    Future<T> Function() fun, {
    required Function(T result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    try {
      if (handler == null) {
        throw Exception(
          "Zeytin has not been started. Use the initialize() command to start Zeytin!",
        );
      } else {
        final result = await fun();
        onSuccess(result);
      }
    } catch (e, s) {
      if (onError != null) {
        onError(e.toString(), s.toString());
      }
    }
  }

  Future<void> getAuditTrail({
    required String targetBoxId,
    required String targetTag,
    required Function(List<Map<String, dynamic>> history) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(() async {
      final auditBoxId = "__audit_$targetBoxId";
      final completer = Completer<List<ZeytinValue>>();
      await getBox(
        boxId: auditBoxId,
        onSuccess: (boxData) => completer.complete(boxData),
        onError: (e, s) => completer.completeError(e),
      );
      final boxData = await completer.future;
      final history = <Map<String, dynamic>>[];

      for (var entry in boxData) {
        if (entry.tag.startsWith("${targetTag}_") && entry.value != null) {
          history.add(entry.value!);
        }
      }
      history.sort(
          (a, b) => (a['timestamp'] as int).compareTo(b['timestamp'] as int));
      onSuccess(history);
    }, onError: onError);
  }

  Future<void> add({
    required ZeytinValue data,
    bool isEncrypt = false,
    Duration? ttl,
    bool keepHistory = false,
    String? auditReason,
    bool sync = false,
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        Map<String, dynamic> finalData = data.value ?? {};
        bool requiresWrapper = isEncrypt || ttl != null;

        if (requiresWrapper) {
          dynamic payloadToSave = data.value ?? {};
          if (isEncrypt) {
            if (encrypter == null) {
              throw Exception("ZeytinCipher has not been started.");
            }
            final rawBytes = BinaryEncoder.encodeMap(data.value ?? {});
            payloadToSave = encrypter!.encode(rawBytes);
          }
          finalData = {
            "_zWrapped": true,
            "_isEncrypted": isEncrypt,
            "data": payloadToSave,
          };
          if (ttl != null) {
            finalData["_expiry"] =
                DateTime.now().add(ttl).millisecondsSinceEpoch;
          }
        }
        if (!keepHistory) {
          await handler!.put(
            truckId: truckID,
            boxId: data.box,
            tag: data.tag,
            value: finalData,
            sync: sync,
          );
          return;
        }
        Map<String, dynamic>? oldData;
        final rawResult = await handler!
            .get(truckId: truckID, boxId: data.box, tag: data.tag);
        if (rawResult != null && rawResult["_zWrapped"] == true) {
          if (rawResult["_isEncrypted"] == true) {
            final encryptedBytes = rawResult["data"] as Uint8List;
            final decryptedBytes = encrypter!.decode(encryptedBytes);
            oldData = BinaryEncoder.decodeMap(decryptedBytes);
          } else {
            oldData = rawResult["data"] as Map<String, dynamic>;
          }
        } else {
          oldData = rawResult;
        }
        final timestamp = DateTime.now().millisecondsSinceEpoch;
        final auditLog = {
          "timestamp": timestamp,
          "reason": auditReason ?? "No reason provided",
          "action": oldData == null ? "CREATE" : "UPDATE",
          "old_value": oldData,
          "new_value": data.value,
        };
        final batchEntries = <String, Map<String, dynamic>>{
          data.tag: finalData,
        };
        await handler!.putBatch(
          truckId: truckID,
          boxId: data.box,
          entries: batchEntries,
        );
        final auditBoxId = "__audit_${data.box}";
        final auditTag = "${data.tag}_$timestamp";

        await handler!.put(
          truckId: truckID,
          boxId: auditBoxId,
          tag: auditTag,
          value: auditLog,
          sync: sync,
        );
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> addBatch({
    required String boxId,
    required List<ZeytinValue> entries,
    bool isEncrypt = false,
    Duration? ttl,
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        Map<String, Map<String, dynamic>> finalEntries = {};
        for (var entry in entries) {
          Map<String, dynamic> entryData = entry.value ?? {};
          bool requiresWrapper = isEncrypt || ttl != null;
          if (requiresWrapper) {
            dynamic payloadToSave = entry.value ?? {};
            if (isEncrypt) {
              if (encrypter == null) {
                throw Exception("ZeytinCipher has not been started.");
              }
              final rawBytes = BinaryEncoder.encodeMap(entry.value ?? {});
              payloadToSave = encrypter!.encode(rawBytes);
            }
            final Map<String, dynamic> wrappedData = {
              "_zWrapped": true,
              "_isEncrypted": isEncrypt,
              "data": payloadToSave,
            };
            if (ttl != null) {
              wrappedData["_expiry"] =
                  DateTime.now().add(ttl).millisecondsSinceEpoch;
            }
            entryData = wrappedData;
          }
          finalEntries[entry.tag] = entryData;
        }
        await handler!.putBatch(
          truckId: truckID,
          boxId: boxId,
          entries: finalEntries,
        );
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> get({
    required String boxId,
    required String tag,
    required Function(ZeytinValue result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async {
        final result = await handler!.get(
          truckId: truckID,
          boxId: boxId,
          tag: tag,
        );
        Map<String, dynamic>? finalResult = result;
        if (result != null && result["_zWrapped"] == true) {
          if (result.containsKey("_expiry")) {
            final expiryTime = result["_expiry"] as int;
            final now = DateTime.now().millisecondsSinceEpoch;
            if (now > expiryTime) {
              handler!.delete(truckId: truckID, boxId: boxId, tag: tag);
              return ZeytinValue(boxId, tag, null);
            }
          }
          if (result["_isEncrypted"] == true) {
            if (encrypter == null) {
              throw Exception(
                "This data is encrypted, but ZeytinCipher hasn't been launched yet!",
              );
            }
            final encryptedBytes = result["data"] as Uint8List;
            final decryptedBytes = encrypter!.decode(encryptedBytes);
            finalResult = BinaryEncoder.decodeMap(decryptedBytes);
          } else {
            finalResult = result["data"] as Map<String, dynamic>;
          }
        }
        return ZeytinValue(boxId, tag, finalResult);
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> getBox({
    required String boxId,
    required Function(List<ZeytinValue> result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async {
        final rawBox = await handler!.getBox(truckId: truckID, boxId: boxId);
        final List<ZeytinValue> validBox = [];
        final now = DateTime.now().millisecondsSinceEpoch;
        for (var entry in rawBox.entries) {
          final val = entry.value;
          Map<String, dynamic>? finalVal = val;
          if (val["_zWrapped"] == true) {
            if (val.containsKey("_expiry") && now > (val["_expiry"] as int)) {
              handler!.delete(truckId: truckID, boxId: boxId, tag: entry.key);
              continue;
            }
            if (val["_isEncrypted"] == true) {
              if (encrypter == null) {
                throw Exception("ZeytinCipher hasn't been launched yet!");
              }
              final encryptedBytes = val["data"] as Uint8List;
              final decryptedBytes = encrypter!.decode(encryptedBytes);
              finalVal = BinaryEncoder.decodeMap(decryptedBytes);
            } else {
              finalVal = val["data"] as Map<String, dynamic>;
            }
          }
          validBox.add(ZeytinValue(boxId, entry.key, finalVal));
        }
        return validBox;
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> addCAS({
    required ZeytinValue data,
    required String casField,
    required dynamic expectedValue,
    bool isEncrypt = false,
    Duration? ttl,
    bool sync = false,
    required Function(bool success) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn<bool>(
      () async {
        Map<String, dynamic> finalData = data.value ?? {};
        bool requiresWrapper = isEncrypt || ttl != null;

        if (requiresWrapper) {
          dynamic payloadToSave = data.value ?? {};
          if (isEncrypt) {
            if (encrypter == null) {
              throw Exception("ZeytinCipher has not been started.");
            }
            final rawBytes = BinaryEncoder.encodeMap(data.value ?? {});
            payloadToSave = encrypter!.encode(rawBytes);
          }
          finalData = {
            "_zWrapped": true,
            "_isEncrypted": isEncrypt,
            "data": payloadToSave,
          };
          if (ttl != null) {
            finalData["_expiry"] =
                DateTime.now().add(ttl).millisecondsSinceEpoch;
          }

          if (data.value != null && data.value!.containsKey(casField)) {
            finalData[casField] = data.value![casField];
          }
        }
        final success = await handler!.putCAS(
          truckId: truckID,
          boxId: data.box,
          tag: data.tag,
          value: finalData,
          casField: casField,
          expectedValue: expectedValue,
          sync: sync,
        );

        return success;
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> getAllBoxes({
    required Function(List<String> result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async => await handler!.getAllBoxes(truckID),
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> getAllTrucks({
    required Function(List<String> result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async => handler!.getAllTruck(),
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> search({
    required String boxId,
    required String field,
    required String prefix,
    required Function(List<ZeytinValue> result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(() async {
      final completer = Completer<List<ZeytinValue>>();
      await getBox(
        boxId: boxId,
        onSuccess: (boxData) => completer.complete(boxData),
        onError: (e, s) => completer.completeError(e),
      );
      final boxData = await completer.future;
      final results = <ZeytinValue>[];
      for (var entry in boxData) {
        if (entry.value != null &&
            entry.value!.containsKey(field) &&
            entry.value![field] is String) {
          if ((entry.value![field] as String).startsWith(prefix)) {
            results.add(entry);
          }
        }
      }
      onSuccess(results);
    }, onError: onError);
  }

  Future<void> filter({
    required String boxId,
    required bool Function(Map<String, dynamic>) predicate,
    required Function(List<ZeytinValue> result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(() async {
      final completer = Completer<List<ZeytinValue>>();
      await getBox(
        boxId: boxId,
        onSuccess: (boxData) => completer.complete(boxData),
        onError: (e, s) => completer.completeError(e),
      );
      final boxData = await completer.future;
      final results = <ZeytinValue>[];

      for (var entry in boxData) {
        if (entry.value != null && predicate(entry.value!)) {
          results.add(entry);
        }
      }
      onSuccess(results);
    }, onError: onError);
  }

  Future<void> contains({
    required String boxId,
    required String tag,
    required Function(bool result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async => await handler!.contains(truckID, boxId, tag),
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> existsTruck({
    required Function(bool result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async => await handler!.existsTruck(truckId: truckID),
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> existsBox({
    required String boxId,
    required Function(bool result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async => await handler!.existsBox(truckId: truckID, boxId: boxId),
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> existsTag({
    required String boxId,
    required String tag,
    required Function(bool result) onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsyncWithReturn(
      () async =>
          await handler!.existsTag(truckId: truckID, boxId: boxId, tag: tag),
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> remove({
    required String boxId,
    required String tag,
    bool sync = false,
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        await handler!.delete(
          truckId: truckID,
          boxId: boxId,
          tag: tag,
          sync: sync,
        );
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> removeBox({
    required String boxId,
    bool sync = false,
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        await handler!.deleteBox(
          truckId: truckID,
          boxId: boxId,
          sync: sync,
        );
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> removeTruck({
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        await handler!.deleteTruck(truckID);
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> compact({
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        await handler!.compactTruck(truckId: truckID);
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> createTruck({
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        await handler!.createTruck(truckId: truckID);
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Future<void> deleteAll({
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        await handler!.deleteAll();
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }

  Stream<Map<String, dynamic>> get changes {
    if (handler == null) {
      throw Exception(
        "Zeytin has not been started. Use the initialize() command to start Zeytin!",
      );
    }
    return handler!.changes;
  }

  Stream<ZeytinValue> watch(String boxId, String tag) async* {
    ZeytinValue? initial;
    await get(boxId: boxId, tag: tag, onSuccess: (res) => initial = res);
    if (initial != null) yield initial!;
    await for (final event in changes) {
      if (event['truckId'] == truckID && event['boxId'] == boxId) {
        final op = event['op'];
        final eventTag = event['tag'];
        final entries = event['entries'] as Map?;

        if (eventTag == tag ||
            op == 'DELETE_BOX' ||
            (op == 'BATCH' && entries != null && entries.containsKey(tag))) {
          ZeytinValue? updated;
          await get(boxId: boxId, tag: tag, onSuccess: (res) => updated = res);
          if (updated != null) yield updated!;
        }
      }
    }
  }

  Stream<List<ZeytinValue>> watchBox(String boxId) async* {
    List<ZeytinValue>? initial;
    await getBox(boxId: boxId, onSuccess: (res) => initial = res);
    if (initial != null) yield initial!;

    await for (final event in changes) {
      if (event['truckId'] == truckID && event['boxId'] == boxId) {
        List<ZeytinValue>? updated;
        await getBox(boxId: boxId, onSuccess: (res) => updated = res);
        if (updated != null) yield updated!;
      }
    }
  }

  Future<void> close({
    Function()? onSuccess,
    Function(String e, String s)? onError,
  }) async {
    await _maybeTryAsync(
      () async {
        await handler!.close();
        handler = null;
      },
      onSuccess: onSuccess,
      onError: onError,
    );
  }
}
