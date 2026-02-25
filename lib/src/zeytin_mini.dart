import 'dart:async';
import 'package:zeytin_local_storage/zeytin_local_storage.dart';

class ZeytinMini {
  static late ZeytinStorage _storage;
  static const String _box = 'mini_box';
  static const String _secret = 'x8y2k9p4m1v7q5z0j3w6t8r2n4b1c9f5';

  static Future<void> init(String basePath) async {
    _storage = ZeytinStorage(
      namespace: 'mini_namespace',
      truckID: 'zeytin_mini',
      encrypter: ZeytinCipher(_secret),
    );
    _storage.initialize(basePath);
    await Future.delayed(const Duration(milliseconds: 300));
  }

  static Future<List<String>> getAllKeys() async {
    final c = Completer<List<String>>();
    await _storage.getBox(
      boxId: _box,
      onSuccess: (result) => c.complete(result.map((e) => e.tag).toList()),
      onError: (e, s) => c.completeError(e),
    );
    return c.future;
  }

  static Future<List<Map<String, dynamic>>> getAllValues() async {
    final c = Completer<List<Map<String, dynamic>>>();
    await _storage.getBox(
      boxId: _box,
      onSuccess: (result) => c.complete(
        result.map((e) => e.value ?? <String, dynamic>{}).toList(),
      ),
      onError: (e, s) => c.completeError(e),
    );
    return c.future;
  }

  static Future<void> add(String key, Map<String, dynamic> value) async {
    final c = Completer<void>();
    await _storage.add(
      data: ZeytinValue(_box, key, value),
      isEncrypt: true,
      onSuccess: c.complete,
      onError: (e, s) => c.completeError(e),
    );
    return c.future;
  }

  static Future<Map<String, dynamic>?> get(String key) async {
    final c = Completer<Map<String, dynamic>?>();
    await _storage.get(
      boxId: _box,
      tag: key,
      onSuccess: (r) => c.complete(r.value),
      onError: (e, s) => c.completeError(e),
    );
    return c.future;
  }

  static Future<void> remove(String key) async {
    final c = Completer<void>();
    await _storage.remove(
      boxId: _box,
      tag: key,
      onSuccess: c.complete,
      onError: (e, s) => c.completeError(e),
    );
    return c.future;
  }

  static Future<bool> contains(String key) async {
    final c = Completer<bool>();
    await _storage.contains(
      boxId: _box,
      tag: key,
      onSuccess: (r) => c.complete(r),
      onError: (e, s) => c.completeError(e),
    );
    return c.future;
  }

  static Future<void> clear() async {
    final c = Completer<void>();
    await _storage.removeBox(
      boxId: _box,
      onSuccess: c.complete,
      onError: (e, s) => c.completeError(e),
    );
    return c.future;
  }
}
