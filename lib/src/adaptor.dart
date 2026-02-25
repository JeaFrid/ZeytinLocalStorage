import 'dart:io';
import 'dart:convert';
import 'dart:typed_data';
import 'dart:async';
import 'dart:isolate';

class ZeytinStorageAdapter {
  final String _basePath;
  final Map<String, ZeytinTruckProxy> _activeTrucks = {};
  final Map<String, Map<String, dynamic>> _memoryCache = {};
  final int _maxCacheSize;
  final List<String> _cacheOrder = [];

  ZeytinStorageAdapter(this._basePath, {int cacheSize = 1000})
    : _maxCacheSize = cacheSize {
    Directory(_basePath).createSync(recursive: true);
  }

  String _getZeytinPath(String truckId) {
    return '$_basePath/${_hashString(truckId)}';
  }

  String _hashString(String input) {
    final bytes = utf8.encode(input);
    final hash = bytes.fold<int>(0, (prev, element) => prev ^ element);
    return hash.toRadixString(16).padLeft(8, '0');
  }

  String _getCacheKey(String truckId, String boxId, String tag) {
    return '${_hashString(truckId)}:${_hashString(boxId)}:${_hashString(tag)}';
  }

  void _updateCache(String key, dynamic value) {
    if (_memoryCache.containsKey(key)) {
      _cacheOrder.remove(key);
    } else if (_cacheOrder.length >= _maxCacheSize) {
      final oldest = _cacheOrder.removeAt(0);
      _memoryCache.remove(oldest);
    }
    _cacheOrder.add(key);
    _memoryCache[key] = value;
  }

  dynamic _getFromCache(String key) {
    if (_memoryCache.containsKey(key)) {
      _cacheOrder.remove(key);
      _cacheOrder.add(key);
      return _memoryCache[key];
    }
    return null;
  }

  Future<ZeytinTruckProxy> _getTruck(String truckId) async {
    if (_activeTrucks.containsKey(truckId)) {
      return _activeTrucks[truckId]!;
    }
    final truck = ZeytinTruckProxy(truckId, _getZeytinPath(truckId));
    await truck.initialize();
    _activeTrucks[truckId] = truck;
    return truck;
  }

  Future<Map<String, Map<String, dynamic>>> readBox({
    required String truckId,
    required String boxId,
  }) async {
    final truck = await _getTruck(truckId);
    return await truck.readBox(boxId);
  }

  Future<void> write({
    required String truckId,
    required String boxId,
    required String tag,
    required Map<String, dynamic> data,
  }) async {
    final truck = await _getTruck(truckId);
    await truck.write(boxId, tag, data);
    _updateCache(_getCacheKey(truckId, boxId, tag), data);
  }

  Future<Map<String, dynamic>?> read({
    required String truckId,
    required String boxId,
    required String tag,
  }) async {
    final cacheKey = _getCacheKey(truckId, boxId, tag);
    final cached = _getFromCache(cacheKey);
    if (cached != null) return Map<String, dynamic>.from(cached);

    final truck = await _getTruck(truckId);
    final data = await truck.read(boxId, tag);
    if (data != null) {
      _updateCache(cacheKey, data);
    }
    return data;
  }

  Future<void> delete({
    required String truckId,
    required String boxId,
    required String tag,
  }) async {
    final truck = await _getTruck(truckId);
    await truck.removeTag(boxId, tag);
    _memoryCache.remove(_getCacheKey(truckId, boxId, tag));
  }

  Future<void> deleteBox({
    required String truckId,
    required String boxId,
  }) async {
    final truck = await _getTruck(truckId);
    final boxData = await truck.readBox(boxId);
    for (final tag in boxData.keys) {
      _memoryCache.remove(_getCacheKey(truckId, boxId, tag));
    }
  }

  Future<void> deleteTruck(String truckId) async {
    if (_activeTrucks.containsKey(truckId)) {
      await _activeTrucks[truckId]!.close();
      _activeTrucks.remove(truckId);
    }
    final path = _getZeytinPath(truckId);
    final dataFile = File('$path.dat');
    final indexFile = File('$path.idx');
    if (await dataFile.exists()) await dataFile.delete();
    if (await indexFile.exists()) await indexFile.delete();
  }

  Future<List<String>> getAllTrucks() async {
    try {
      final dir = Directory(_basePath);
      return await dir
          .list()
          .where((entity) => entity is File && entity.path.endsWith('.dat'))
          .map((entity) => entity.path.split('/').last.replaceAll('.dat', ''))
          .toList();
    } catch (_) {
      return [];
    }
  }

  final StreamController<Map<String, dynamic>> _changeController =
      StreamController<Map<String, dynamic>>.broadcast();
  Stream<Map<String, dynamic>> get changes => _changeController.stream;

  void _notifyChange({
    required String truckId,
    required String boxId,
    String? tag,
    required String op,
    Map<String, dynamic>? value,
    Map<String, Map<String, dynamic>>? entries,
  }) {
    final event = <String, dynamic>{
      'truckId': truckId,
      'boxId': boxId,
      'op': op,
    };
    if (tag != null) event['tag'] = tag;
    if (value != null) event['value'] = value;
    if (entries != null) event['entries'] = entries;
    _changeController.add(event);
  }

  Future<void> putBatch({
    required String truckId,
    required String boxId,
    required Map<String, Map<String, dynamic>> entries,
  }) async {
    final truck = await _getTruck(truckId);
    await truck.batch(boxId, entries);
    for (final entry in entries.entries) {
      _updateCache(_getCacheKey(truckId, boxId, entry.key), entry.value);
    }
    _notifyChange(
      truckId: truckId,
      boxId: boxId,
      op: 'BATCH',
      entries: entries,
    );
  }

  Future<List<Map<String, dynamic>>> search({
    required String truckId,
    required String boxId,
    required String field,
    required String prefix,
  }) async {
    final truck = await _getTruck(truckId);
    return await truck.query(boxId, field, prefix);
  }

  Future<List<Map<String, dynamic>>> filter({
    required String truckId,
    required String boxId,
    required bool Function(Map<String, dynamic>) predicate,
  }) async {
    final truck = await _getTruck(truckId);
    final boxData = await truck.readBox(boxId);
    final results = <Map<String, dynamic>>[];
    for (final entry in boxData.values) {
      if (predicate(entry)) {
        results.add(entry);
      }
    }
    return results;
  }

  Future<void> compactTruck({required String truckId}) async {
    if (_activeTrucks.containsKey(truckId)) {
      final truck = _activeTrucks[truckId]!;
      await truck.compact();
      _memoryCache.clear();
      _cacheOrder.clear();
    }
  }

  Future<bool> contains({
    required String truckId,
    required String boxId,
    required String tag,
  }) async {
    final cacheKey = _getCacheKey(truckId, boxId, tag);
    if (_memoryCache.containsKey(cacheKey)) return true;
    final truck = await _getTruck(truckId);
    return await truck.contains(boxId, tag);
  }

  Future<bool> existsTruck({required String truckId}) async {
    final path = _getZeytinPath(truckId);
    final dataFile = File('$path.dat');
    try {
      return await dataFile.exists();
    } catch (_) {
      return false;
    }
  }

  Future<bool> existsBox({
    required String truckId,
    required String boxId,
  }) async {
    if (!await existsTruck(truckId: truckId)) return false;
    final truck = await _getTruck(truckId);
    final boxData = await truck.readBox(boxId);
    return boxData.isNotEmpty;
  }

  Future<bool> existsTag({
    required String truckId,
    required String boxId,
    required String tag,
  }) async {
    final cacheKey = _getCacheKey(truckId, boxId, tag);
    if (_memoryCache.containsKey(cacheKey)) return true;
    if (!await existsTruck(truckId: truckId)) return false;
    final truck = await _getTruck(truckId);
    return await truck.contains(boxId, tag);
  }

  Future<void> createTruck({required String truckId}) async {
    if (await existsTruck(truckId: truckId)) return;
    final truck = ZeytinTruckProxy(truckId, _getZeytinPath(truckId));
    await truck.initialize();
    _activeTrucks[truckId] = truck;
  }

  Future<void> deleteAll() async {
    for (final truck in _activeTrucks.values) {
      await truck.close();
    }
    _activeTrucks.clear();
    _memoryCache.clear();
    _cacheOrder.clear();

    final dir = Directory(_basePath);
    if (await dir.exists()) {
      try {
        await dir.delete(recursive: true);
        await dir.create(recursive: true);
      } catch (_) {}
    }
  }

  Future<void> close() async {
    for (final truck in _activeTrucks.values) {
      await truck.close();
    }
    _activeTrucks.clear();
    _memoryCache.clear();
    _cacheOrder.clear();
  }
}

class ZeytinTruckProxy {
  final String id;
  final String path;
  late SendPort _sendPort;
  final Map<int, Completer<dynamic>> _completers = {};
  int _messageId = 0;
  final ReceivePort _receivePort = ReceivePort();

  ZeytinTruckProxy(this.id, this.path);

  Future<void> initialize() async {
    final completer = Completer<void>();
    await Isolate.spawn(_startIsolate, _receivePort.sendPort);
    _receivePort.listen((message) {
      if (message is SendPort) {
        _sendPort = message;
        _sendCommand('init', {'id': id, 'path': path})
            .then((_) {
              if (!completer.isCompleted) completer.complete();
            })
            .catchError((e) {
              if (!completer.isCompleted) completer.completeError(e);
            });
      } else if (message is Map) {
        final msgId = message['id'] as int;
        final completer = _completers[msgId];
        if (completer != null) {
          if (message.containsKey('result')) {
            completer.complete(message['result']);
          } else if (message.containsKey('error')) {
            completer.completeError(Exception(message['error']));
          }
          _completers.remove(msgId);
        }
      }
    });
    return completer.future;
  }

  Future<void> batch(String boxId, Map<String, Map<String, dynamic>> entries) =>
      _sendCommand('batch', {'boxId': boxId, 'entries': entries});

  Future<List<Map<String, dynamic>>> query(
    String boxId,
    String field,
    String prefix,
  ) async {
    return await _sendCommand('query', {
          'boxId': boxId,
          'field': field,
          'prefix': prefix,
        })
        as List<Map<String, dynamic>>;
  }

  Future<List<Map<String, dynamic>>> queryAdvanced({
    required String boxId,
    bool Function(Map<String, dynamic>)? filter,
  }) async {
    final boxData = await readBox(boxId);
    final results = <Map<String, dynamic>>[];
    for (final entry in boxData.values) {
      if (filter == null || filter(entry)) {
        results.add(entry);
      }
    }
    return results;
  }

  Future<void> compact() => _sendCommand('compact', {});

  Future<bool> contains(String boxId, String tag) async {
    return await _sendCommand('contains', {'boxId': boxId, 'tag': tag}) as bool;
  }

  static void _startIsolate(SendPort sendPort) {
    final receivePort = ReceivePort();
    sendPort.send(receivePort.sendPort);
    final engine = _ZeytinTruckEngine();

    receivePort.listen((message) async {
      if (message is Map) {
        final command = message['command'] as String;
        final params = message['params'] as Map<String, dynamic>;
        final id = message['id'] as int;

        try {
          dynamic result;
          switch (command) {
            case 'init':
              await engine.init(params['id'], params['path']);
              result = null;
              break;
            case 'write':
              await engine.write(
                params['boxId'],
                params['tag'],
                Map<String, dynamic>.from(params['value']),
              );
              result = null;
              break;
            case 'batch':
              await engine.batch(
                params['boxId'],
                Map<String, Map<String, dynamic>>.from(params['entries']),
              );
              result = null;
              break;
            case 'query':
              result = await engine.query(
                params['boxId'],
                params['field'],
                params['prefix'],
              );
              break;
            case 'compact':
              await engine.compact();
              result = null;
              break;
            case 'contains':
              result = await engine.contains(params['boxId'], params['tag']);
              break;
            case 'read':
              result = await engine.read(params['boxId'], params['tag']);
              break;
            case 'readBox':
              result = await engine.readBox(params['boxId']);
              break;
            case 'removeTag':
              await engine.removeTag(params['boxId'], params['tag']);
              result = null;
              break;
            case 'close':
              await engine.close();
              result = null;
              break;
          }
          sendPort.send({'id': id, 'result': result});
        } catch (e) {
          sendPort.send({'id': id, 'error': e.toString()});
        }
      }
    });
  }

  Future<dynamic> _sendCommand(String command, Map<String, dynamic> params) {
    final id = _messageId++;
    final completer = Completer<dynamic>();
    _completers[id] = completer;
    _sendPort.send({'command': command, 'params': params, 'id': id});
    return completer.future;
  }

  Future<void> write(String boxId, String tag, Map<String, dynamic> value) =>
      _sendCommand('write', {'boxId': boxId, 'tag': tag, 'value': value});

  Future<Map<String, dynamic>?> read(String boxId, String tag) async {
    return await _sendCommand('read', {'boxId': boxId, 'tag': tag})
        as Map<String, dynamic>?;
  }

  Future<Map<String, Map<String, dynamic>>> readBox(String boxId) async {
    return await _sendCommand('readBox', {'boxId': boxId})
        as Map<String, Map<String, dynamic>>;
  }

  Future<void> removeTag(String boxId, String tag) =>
      _sendCommand('removeTag', {'boxId': boxId, 'tag': tag});

  Future<void> close() => _sendCommand('close', {});
}

class _ZeytinTruckEngine {
  late _ZeytinStorageEngine _engine;

  Future<void> init(String id, String path) async {
    _engine = _ZeytinStorageEngine(id, path);
    await _engine.initialize();
  }

  Future<void> write(String boxId, String tag, Map<String, dynamic> value) =>
      _engine.write(boxId, tag, value);

  Future<Map<String, dynamic>?> read(String boxId, String tag) =>
      _engine.read(boxId, tag);

  Future<Map<String, Map<String, dynamic>>> readBox(String boxId) =>
      _engine.readBox(boxId);

  Future<void> removeTag(String boxId, String tag) =>
      _engine.removeTag(boxId, tag);
  Future<void> batch(String boxId, Map<String, Map<String, dynamic>> entries) =>
      _engine.batch(boxId, entries);

  Future<List<Map<String, dynamic>>> query(
    String boxId,
    String field,
    String prefix,
  ) => _engine.query(boxId, field, prefix);

  Future<void> compact() => _engine.compact();

  Future<bool> contains(String boxId, String tag) =>
      _engine.contains(boxId, tag);
  Future<void> close() => _engine.close();
}

class _ZeytinStorageEngine {
  final String id;
  final String path;
  final Map<String, Map<String, List<int>>> _index = {};
  final Map<String, Map<String, dynamic>> _cache = {};
  RandomAccessFile? _writer;
  RandomAccessFile? _reader;
  bool _isDirty = false;

  _ZeytinStorageEngine(this.id, this.path);

  File get _dataFile => File('$path.dat');
  File get _indexFile => File('$path.idx');

  Future<void> initialize() async {
    await _loadIndex();
    if (await _dataFile.exists()) {
      _writer = await _dataFile.open(mode: FileMode.append);
    }
  }

  Future<void> _loadIndex() async {
    if (await _indexFile.exists()) {
      try {
        final bytes = await _indexFile.readAsBytes();
        if (bytes.isNotEmpty) {
          _deserializeIndex(bytes);
        }
      } catch (_) {}
    }
  }

  Future<void> _saveIndex() async {
    if (!_isDirty) return;
    final bytes = _serializeIndex();
    await _indexFile.writeAsBytes(bytes);
    _isDirty = false;
  }

  Uint8List _serializeIndex() {
    final builder = BytesBuilder();
    final boxIds = _index.keys.toList();

    builder.add(_int32Bytes(boxIds.length));

    for (final boxId in boxIds) {
      final boxBytes = utf8.encode(boxId);
      builder.add(_int32Bytes(boxBytes.length));
      builder.add(boxBytes);

      final tags = _index[boxId]!;
      builder.add(_int32Bytes(tags.length));

      for (final entry in tags.entries) {
        final tagBytes = utf8.encode(entry.key);
        builder.add(_int32Bytes(tagBytes.length));
        builder.add(tagBytes);

        final addrBytes = ByteData(8);
        addrBytes.setUint32(0, entry.value[0], Endian.little);
        addrBytes.setUint32(4, entry.value[1], Endian.little);
        builder.add(addrBytes.buffer.asUint8List());
      }
    }
    return builder.toBytes();
  }

  void _deserializeIndex(Uint8List bytes) {
    final reader = ByteData.view(bytes.buffer);
    int offset = 0;

    if (bytes.length < 4) return;
    final boxCount = reader.getUint32(offset, Endian.little);
    offset += 4;

    for (int i = 0; i < boxCount; i++) {
      final boxLen = reader.getUint32(offset, Endian.little);
      offset += 4;
      final boxId = utf8.decode(
        Uint8List.view(reader.buffer, reader.offsetInBytes + offset, boxLen),
      );
      offset += boxLen;

      final tagCount = reader.getUint32(offset, Endian.little);
      offset += 4;

      final boxMap = <String, List<int>>{};
      for (int j = 0; j < tagCount; j++) {
        final tagLen = reader.getUint32(offset, Endian.little);
        offset += 4;
        final tag = utf8.decode(
          Uint8List.view(reader.buffer, reader.offsetInBytes + offset, tagLen),
        );
        offset += tagLen;

        final addrOffset = reader.getUint32(offset, Endian.little);
        offset += 4;
        final addrLen = reader.getUint32(offset, Endian.little);
        offset += 4;

        boxMap[tag] = [addrOffset, addrLen];
      }
      _index[boxId] = boxMap;
    }
  }

  Uint8List _int32Bytes(int value) {
    final bytes = ByteData(4);
    bytes.setUint32(0, value, Endian.little);
    return bytes.buffer.asUint8List();
  }

  Future<void> write(
    String boxId,
    String tag,
    Map<String, dynamic> value,
  ) async {
    _writer ??= await _dataFile.open(mode: FileMode.append);

    final offset = await _writer!.length();
    final bytes = _encode(boxId, tag, value);

    await _writer!.writeFrom(bytes);
    await _writer!.flush();

    _index[boxId] ??= {};
    _index[boxId]![tag] = [offset, bytes.length];
    _cache['$boxId:$tag'] = value;
    _isDirty = true;

    await _saveIndex();
  }

  Future<Map<String, dynamic>?> read(String boxId, String tag) async {
    final cacheKey = '$boxId:$tag';
    if (_cache.containsKey(cacheKey)) {
      return Map.from(_cache[cacheKey]!);
    }

    final addr = _index[boxId]?[tag];
    if (addr == null) return null;

    _reader ??= await _dataFile.open(mode: FileMode.read);
    await _reader!.setPosition(addr[0]);
    final block = await _reader!.read(addr[1]);

    try {
      final data = _decode(block);
      _cache[cacheKey] = data;
      return Map.from(data);
    } catch (_) {
      return null;
    }
  }

  Future<Map<String, Map<String, dynamic>>> readBox(String boxId) async {
    final result = <String, Map<String, dynamic>>{};
    final boxData = _index[boxId];
    if (boxData == null) return result;

    for (final tag in boxData.keys) {
      final data = await read(boxId, tag);
      if (data != null) {
        result[tag] = data;
      }
    }
    return result;
  }

  Future<void> removeTag(String boxId, String tag) async {
    _index[boxId]?.remove(tag);
    _cache.remove('$boxId:$tag');
    _isDirty = true;
    await _saveIndex();
  }

  Future<void> batch(
    String boxId,
    Map<String, Map<String, dynamic>> entries,
  ) async {
    _writer ??= await _dataFile.open(mode: FileMode.append);
    var offset = await _writer!.length();

    for (final entry in entries.entries) {
      final bytes = _encode(boxId, entry.key, entry.value);
      await _writer!.writeFrom(bytes);

      _index[boxId] ??= {};
      _index[boxId]![entry.key] = [offset, bytes.length];
      _cache['$boxId:${entry.key}'] = entry.value;

      offset += bytes.length;
      _isDirty = true;
    }
    await _writer!.flush();
    await _saveIndex();
  }

  Future<List<Map<String, dynamic>>> query(
    String boxId,
    String field,
    String prefix,
  ) async {
    final results = <Map<String, dynamic>>[];
    final boxData = _index[boxId];
    if (boxData == null) return results;

    for (final tag in boxData.keys) {
      final data = await read(boxId, tag);
      if (data != null) {
        if (data.containsKey(field) && data[field] is String) {
          if ((data[field] as String).startsWith(prefix)) {
            results.add(data);
          }
        }
      }
    }
    return results;
  }

  Future<void> compact() async {
    final tempFile = File('${path}_temp.dat');
    final tempIndex = <String, Map<String, List<int>>>{};
    final sink = tempFile.openWrite();
    int offset = 0;

    for (final boxId in _index.keys) {
      for (final tag in _index[boxId]!.keys) {
        final data = await read(boxId, tag);
        if (data != null) {
          final bytes = _encode(boxId, tag, data);
          sink.add(bytes);
          tempIndex[boxId] ??= {};
          tempIndex[boxId]![tag] = [offset, bytes.length];
          offset += bytes.length;
        }
      }
    }

    await sink.flush();
    await sink.close();
    await _reader?.close();
    await _writer?.close();

    _reader = null;
    _writer = null;

    final oldDataFile = _dataFile;
    final oldIndexFile = _indexFile;

    if (await oldDataFile.exists()) await oldDataFile.delete();
    if (await oldIndexFile.exists()) await oldIndexFile.delete();

    await tempFile.rename(oldDataFile.path);

    _index.clear();
    _index.addAll(tempIndex);
    _isDirty = true;
    await _saveIndex();

    _writer = await _dataFile.open(mode: FileMode.append);
  }

  Future<bool> contains(String boxId, String tag) async {
    if (_cache.containsKey('$boxId:$tag')) return true;
    return _index[boxId]?.containsKey(tag) ?? false;
  }

  Uint8List _encode(String boxId, String tag, Map<String, dynamic> data) {
    final builder = BytesBuilder();

    builder.addByte(0xDB);

    final boxBytes = utf8.encode(boxId);
    builder.add(_int32Bytes(boxBytes.length));
    builder.add(boxBytes);

    final tagBytes = utf8.encode(tag);
    builder.add(_int32Bytes(tagBytes.length));
    builder.add(tagBytes);

    final dataBytes = _encodeMap(data);
    builder.add(_int32Bytes(dataBytes.length));
    builder.add(dataBytes);

    return builder.toBytes();
  }

  Uint8List _encodeMap(Map<String, dynamic> map) {
    final builder = BytesBuilder();
    _encodeValue(builder, map);
    return builder.toBytes();
  }

  void _encodeValue(BytesBuilder builder, dynamic value) {
    if (value == null) {
      builder.addByte(0);
    } else if (value is bool) {
      builder.addByte(1);
      builder.addByte(value ? 1 : 0);
    } else if (value is int) {
      builder.addByte(2);
      final bytes = ByteData(8);
      bytes.setInt64(0, value, Endian.little);
      builder.add(bytes.buffer.asUint8List());
    } else if (value is double) {
      builder.addByte(3);
      final bytes = ByteData(8);
      bytes.setFloat64(0, value, Endian.little);
      builder.add(bytes.buffer.asUint8List());
    } else if (value is String) {
      builder.addByte(4);
      final strBytes = utf8.encode(value);
      builder.add(_int32Bytes(strBytes.length));
      builder.add(strBytes);
    } else if (value is List) {
      builder.addByte(5);
      builder.add(_int32Bytes(value.length));
      for (final item in value) {
        _encodeValue(builder, item);
      }
    } else if (value is Map) {
      builder.addByte(6);
      builder.add(_int32Bytes(value.length));
      value.forEach((k, v) {
        _encodeValue(builder, k.toString());
        _encodeValue(builder, v);
      });
    }
  }

  Map<String, dynamic> _decode(Uint8List bytes) {
    final reader = ByteData.view(bytes.buffer);
    final result = _decodeValue(reader, 1).value;
    return Map<String, dynamic>.from(result as Map);
  }

  MapEntry<int, dynamic> _decodeValue(ByteData reader, int offset) {
    final type = reader.getUint8(offset++);

    switch (type) {
      case 0:
        return MapEntry(offset, null);
      case 1:
        return MapEntry(offset + 1, reader.getUint8(offset) == 1);
      case 2:
        return MapEntry(offset + 8, reader.getInt64(offset, Endian.little));
      case 3:
        return MapEntry(offset + 8, reader.getFloat64(offset, Endian.little));
      case 4:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final str = utf8.decode(
          Uint8List.view(reader.buffer, reader.offsetInBytes + offset, len),
        );
        return MapEntry(offset + len, str);
      case 5:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final list = [];
        for (int i = 0; i < len; i++) {
          final res = _decodeValue(reader, offset);
          offset = res.key;
          list.add(res.value);
        }
        return MapEntry(offset, list);
      case 6:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final map = <String, dynamic>{};
        for (int i = 0; i < len; i++) {
          final keyRes = _decodeValue(reader, offset);
          final valRes = _decodeValue(reader, keyRes.key);
          offset = valRes.key;
          map[keyRes.value.toString()] = valRes.value;
        }
        return MapEntry(offset, map);
      default:
        throw FormatException('Unknown type: $type');
    }
  }

  Future<void> close() async {
    await _saveIndex();
    await _reader?.close();
    await _writer?.close();
  }
}
