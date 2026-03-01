import 'dart:io';
import 'dart:convert';
import 'dart:typed_data';
import 'dart:async';
import 'dart:isolate';

class LRUCache<K, V> {
  final int maxSize;
  final Map<K, _Node<V>> _map = {};
  _Node<V>? _head;
  _Node<V>? _tail;
  int _size = 0;
  LRUCache(this.maxSize);
  V? get(K key) {
    final node = _map[key];
    if (node == null) return null;
    _moveToHead(node);
    return node.value;
  }

  void put(K key, V value) {
    if (_map.containsKey(key)) {
      final node = _map[key]!;
      node.value = value;
      _moveToHead(node);
    } else {
      final node = _Node<V>(key, value);
      _map[key] = node;
      _addToHead(node);
      _size++;
      if (_size > maxSize) _removeTail();
    }
  }

  bool contains(K key) => _map.containsKey(key);
  void remove(K key) {
    final node = _map[key];
    if (node != null) {
      _removeNode(node);
      _map.remove(key);
      _size--;
    }
  }

  void clear() {
    _map.clear();
    _head = _tail = null;
    _size = 0;
  }

  void _moveToHead(_Node<V> node) {
    if (node == _head) return;
    _removeNode(node);
    _addToHead(node);
  }

  void _addToHead(_Node<V> node) {
    node.prev = null;
    node.next = _head;
    if (_head != null) _head!.prev = node;
    _head = node;
    _tail ??= node;
  }

  void _removeNode(_Node<V> node) {
    if (node.prev != null) {
      node.prev!.next = node.next;
    } else {
      _head = node.next;
    }
    if (node.next != null) {
      node.next!.prev = node.prev;
    } else {
      _tail = node.prev;
    }
    node.prev = null;
    node.next = null;
  }

  void _removeTail() {
    if (_tail != null) {
      final key = _tail!.key;
      _removeNode(_tail!);
      _map.remove(key);
      _size--;
    }
  }
}

class _Node<V> {
  final dynamic key;
  V value;
  _Node<V>? prev;
  _Node<V>? next;
  _Node(this.key, this.value);
}

class BinaryEncoder {
  static const int typeNULL = 0,
      typeBOOL = 1,
      typeINT = 2,
      typeDOUBLE = 3,
      typeSTRING = 4,
      typeLIST = 5,
      typeMAP = 6,
      typeDATETIME = 7,
      typeUINT8LIST = 8,
      typeBIGINT = 9,
      magicByteV1 = 0xDB,
      magicByteV2 = 0xDC;

  static Uint8List encode(
      String boxId, String tag, Map<String, dynamic>? data) {
    final builder = BytesBuilder();
    builder.addByte(magicByteV2);

    final boxBytes = utf8.encode(boxId);
    _encodeRawLength(builder, boxBytes.length);
    builder.add(boxBytes);

    final tagBytes = utf8.encode(tag);
    _encodeRawLength(builder, tagBytes.length);
    builder.add(tagBytes);

    if (data == null) {
      _encodeRawLength(builder, 0);
    } else {
      final dataBytes = encodeMap(data);
      _encodeRawLength(builder, dataBytes.length);
      builder.add(dataBytes);
    }

    final payload = builder.toBytes();
    final crc = calculateCRC32(payload);

    final finalBuilder = BytesBuilder();
    finalBuilder.add(payload);

    final crcBytes = ByteData(4)..setUint32(0, crc, Endian.little);
    finalBuilder.add(crcBytes.buffer.asUint8List());

    return finalBuilder.toBytes();
  }

  static int calculateCRC32(Uint8List bytes) {
    int crc = 0xFFFFFFFF;
    for (int i = 0; i < bytes.length; i++) {
      crc ^= bytes[i];
      for (int j = 0; j < 8; j++) {
        if ((crc & 1) != 0) {
          crc = (crc >> 1) ^ 0xEDB88320;
        } else {
          crc >>= 1;
        }
      }
    }
    return crc ^ 0xFFFFFFFF;
  }

  static Uint8List encodeMap(Map<String, dynamic> data) {
    final builder = BytesBuilder();
    _encodeValue(builder, data);
    return builder.toBytes();
  }

  static Map<String, dynamic> decodeMap(Uint8List bytes) {
    final reader = ByteData.sublistView(bytes);
    return _decodeValue(reader, 0).value as Map<String, dynamic>;
  }

  static Map<String, dynamic> decodeValue(Uint8List bytes) {
    final reader = ByteData.sublistView(bytes);
    return _decodeValue(reader, 0).value as Map<String, dynamic>;
  }

  static void _encodeValue(BytesBuilder builder, dynamic value) {
    if (value == null) {
      builder.addByte(typeNULL);
    } else if (value is bool) {
      builder.addByte(typeBOOL);
      builder.addByte(value ? 1 : 0);
    } else if (value is int) {
      builder.addByte(typeINT);
      final bytes = ByteData(8);
      bytes.setInt64(0, value, Endian.little);
      builder.add(bytes.buffer.asUint8List());
    } else if (value is double) {
      builder.addByte(typeDOUBLE);
      final bytes = ByteData(8);
      bytes.setFloat64(0, value, Endian.little);
      builder.add(bytes.buffer.asUint8List());
    } else if (value is String) {
      builder.addByte(typeSTRING);
      final utf8Bytes = utf8.encode(value);
      _encodeRawLength(builder, utf8Bytes.length);
      builder.add(utf8Bytes);
    } else if (value is Uint8List) {
      builder.addByte(typeUINT8LIST);
      _encodeRawLength(builder, value.length);
      builder.add(value);
    } else if (value is List) {
      builder.addByte(typeLIST);
      _encodeRawLength(builder, value.length);
      for (var item in value) {
        _encodeValue(builder, item);
      }
    } else if (value is Map<String, dynamic>) {
      builder.addByte(typeMAP);
      _encodeRawLength(builder, value.length);
      for (var entry in value.entries) {
        _encodeValue(builder, entry.key);
        _encodeValue(builder, entry.value);
      }
    } else if (value is DateTime) {
      builder.addByte(typeDATETIME);
      final bytes = ByteData(8);
      bytes.setInt64(0, value.millisecondsSinceEpoch, Endian.little);
      builder.add(bytes.buffer.asUint8List());
    } else if (value is BigInt) {
      builder.addByte(typeBIGINT);
      final utf8Bytes = utf8.encode(value.toString());
      _encodeRawLength(builder, utf8Bytes.length);
      builder.add(utf8Bytes);
    } else {
      throw ArgumentError('Unsupported type: ${value.runtimeType}');
    }
  }

  static MapEntry<int, dynamic> _decodeValue(ByteData reader, int offset) {
    final type = reader.getUint8(offset++);
    switch (type) {
      case typeNULL:
        return MapEntry(offset, null);
      case typeBOOL:
        return MapEntry(offset + 1, reader.getUint8(offset) == 1);
      case typeINT:
        return MapEntry(offset + 8, reader.getInt64(offset, Endian.little));
      case typeDOUBLE:
        return MapEntry(offset + 8, reader.getFloat64(offset, Endian.little));
      case typeDATETIME:
        final millis = reader.getInt64(offset, Endian.little);
        return MapEntry(
          offset + 8,
          DateTime.fromMillisecondsSinceEpoch(millis),
        );
      case typeSTRING:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final val = utf8.decode(
          reader.buffer.asUint8List(reader.offsetInBytes + offset, len),
        );
        return MapEntry(offset + len, val);

      case typeBIGINT:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final valStr = utf8.decode(
          reader.buffer.asUint8List(reader.offsetInBytes + offset, len),
        );
        return MapEntry(offset + len, BigInt.parse(valStr));

      case typeUINT8LIST:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final val = Uint8List.fromList(
          reader.buffer.asUint8List(reader.offsetInBytes + offset, len),
        );
        return MapEntry(offset + len, val);
      case typeLIST:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final list = [];
        for (var i = 0; i < len; i++) {
          final res = _decodeValue(reader, offset);
          offset = res.key;
          list.add(res.value);
        }
        return MapEntry(offset, list);
      case typeMAP:
        final len = reader.getUint32(offset, Endian.little);
        offset += 4;
        final map = <String, dynamic>{};
        for (var i = 0; i < len; i++) {
          final kRes = _decodeValue(reader, offset);
          if (kRes.value is! String) {
            throw FormatException('Map key must be String');
          }
          final vRes = _decodeValue(reader, kRes.key);
          offset = vRes.key;
          map[kRes.value as String] = vRes.value;
        }
        return MapEntry(offset, map);
      default:
        throw FormatException('Unknown type: $type');
    }
  }

  static void _encodeRawLength(BytesBuilder builder, int length) {
    final bytes = ByteData(4);
    bytes.setUint32(0, length, Endian.little);
    builder.add(bytes.buffer.asUint8List());
  }
}

class PersistentIndex {
  final File _file;
  Map<String, Map<String, List<int>>> _index = {};
  PersistentIndex(String path) : _file = File(path);
  Future<void> load() async {
    if (await _file.exists()) {
      try {
        final bytes = await _file.readAsBytes();
        if (bytes.isNotEmpty) _index = _deserializeIndex(bytes);
      } catch (e) {
        print(e.toString());
        _index = {};
      }
    }
  }

  Future<void> save() async {
    final bytes = _serializeIndex(_index);
    await _file.writeAsBytes(bytes, flush: true);
  }

  void update(String boxId, String tag, int offset, int length) {
    _index[boxId] ??= {};
    _index[boxId]![tag] = [offset, length];
  }

  List<int>? get(String boxId, String tag) => _index[boxId]?[tag];
  Map<String, List<int>>? getBox(String boxId) => _index[boxId];
  int getMaxIndexedOffset() {
    int maxOffset = 0;
    for (var box in _index.values) {
      for (var addr in box.values) {
        if (addr[0] + addr[1] > maxOffset) maxOffset = addr[0] + addr[1];
      }
    }
    return maxOffset;
  }

  Uint8List _serializeIndex(Map<String, Map<String, List<int>>> index) {
    final builder = BytesBuilder();
    final boxIds = index.keys.toList();
    builder.add(
      (ByteData(
        4,
      )..setUint32(0, boxIds.length, Endian.little))
          .buffer
          .asUint8List(),
    );
    for (var bId in boxIds) {
      final bBytes = utf8.encode(bId);
      builder.add(
        (ByteData(
          4,
        )..setUint32(0, bBytes.length, Endian.little))
            .buffer
            .asUint8List(),
      );
      builder.add(bBytes);
      final tags = index[bId]!;
      builder.add(
        (ByteData(
          4,
        )..setUint32(0, tags.length, Endian.little))
            .buffer
            .asUint8List(),
      );
      for (var entry in tags.entries) {
        final tBytes = utf8.encode(entry.key);
        builder.add(
          (ByteData(
            4,
          )..setUint32(0, tBytes.length, Endian.little))
              .buffer
              .asUint8List(),
        );
        builder.add(tBytes);
        final addr = ByteData(8);
        addr.setUint32(0, entry.value[0], Endian.little);
        addr.setUint32(4, entry.value[1], Endian.little);
        builder.add(addr.buffer.asUint8List());
      }
    }
    return builder.toBytes();
  }

  Map<String, Map<String, List<int>>> _deserializeIndex(Uint8List bytes) {
    final res = <String, Map<String, List<int>>>{};
    final reader = ByteData.sublistView(bytes);
    int offset = 0;
    if (bytes.length < 4) return res;
    final bCount = reader.getUint32(offset, Endian.little);
    offset += 4;
    for (var i = 0; i < bCount; i++) {
      final bLen = reader.getUint32(offset, Endian.little);
      offset += 4;
      final bId = utf8.decode(
        reader.buffer.asUint8List(reader.offsetInBytes + offset, bLen),
      );
      offset += bLen;
      final tCount = reader.getUint32(offset, Endian.little);
      offset += 4;
      final bMap = <String, List<int>>{};
      for (var j = 0; j < tCount; j++) {
        final tLen = reader.getUint32(offset, Endian.little);
        offset += 4;
        final t = utf8.decode(
          reader.buffer.asUint8List(reader.offsetInBytes + offset, tLen),
        );
        offset += tLen;
        final aOff = reader.getUint32(offset, Endian.little);
        offset += 4;
        final aLen = reader.getUint32(offset, Endian.little);
        offset += 4;
        bMap[t] = [aOff, aLen];
      }
      res[bId] = bMap;
    }
    return res;
  }
}

class Truck {
  final String id, path;
  final PersistentIndex _index;
  final LRUCache<String, Map<String, dynamic>> _cache;
  final Map<String, Map<String, Map<String, Set<String>>>> _fieldIndex = {};
  final Map<String, Map<String, dynamic>?> _writeBuffer = {};
  Timer? _flushTimer;
  bool _isFlushing = false;
  final int _flushCountThreshold = 100;
  final Duration _flushTimeThreshold = const Duration(milliseconds: 500);
  int _compactCounter = 0, _dirtyCount = 0;
  final int _compactThreshold = 500, _saveThreshold = 500;
  bool _isCompacting = false, _isSavingIndex = false;
  RandomAccessFile? _reader, _writer;
  Future<void> _lock = Future.value();
  Truck(this.id, this.path)
      : _index = PersistentIndex('$path/$id.idx'),
        _cache = LRUCache(10000);
  File get _dataFile => File('$path/$id.dat');
  Future<void> initialize() async {
    try {
      await _index.load();
      if (await _dataFile.exists()) {
        await _repair();
        await _rebuildSearchIndex();
        if (await _needsMigration()) {
          print(
              "Zeytin: Eski veri formatı (V1) tespit edildi. Yeni formata (V2) sessiz göç başlatılıyor...");
          print("Zeytin: Veritabanı başarıyla yeni mimariye güncellendi.");
        }

        _writer = await _dataFile.open(mode: FileMode.append);
      }
    } catch (e, stack) {
      print("Truck Initialize Error: $e\n$stack");
    }
  }

  Future<bool> _needsMigration() async {
    if (_index._index.isEmpty) return false;
    try {
      final raf = await _dataFile.open(mode: FileMode.read);
      int firstOffset = -1;
      for (var box in _index._index.values) {
        for (var addr in box.values) {
          if (firstOffset == -1 || addr[0] < firstOffset) {
            firstOffset = addr[0];
          }
        }
      }

      if (firstOffset != -1) {
        await raf.setPosition(firstOffset);
        final magicList = await raf.read(1);
        await raf.close();
        if (magicList.isNotEmpty && magicList[0] == BinaryEncoder.magicByteV1) {
          return true;
        }
      } else {
        await raf.close();
      }
    } catch (e) {
      print("Zeytin Migration Check Error: $e");
    }
    return false;
  }

  Future<T> _synchronized<T>(Future<T> Function() action) {
    final result = _lock.then((_) => action());
    _lock = result.catchError((_) {}).then((_) => null);
    return result;
  }

  Future<Map<String, dynamic>?> _readInternal(String bId, String t) async {
    final key = '$bId:$t';
    if (_writeBuffer.containsKey(key)) {
      return _writeBuffer[key];
    }
    final c = _cache.get(key);
    if (c != null) return c;
    final addr = _index.get(bId, t);
    if (addr == null) return null;
    try {
      _reader ??= await _dataFile.open(mode: FileMode.read);
      await _reader!.setPosition(addr[0]);
      final block = await _reader!.read(addr[1]);
      if (block.length < addr[1] || block.isEmpty) return null;
      final magicByte = block[0];
      ByteData blockReader;
      if (magicByte == BinaryEncoder.magicByteV2) {
        if (block.length < 5) return null;
        final payload = block.sublist(0, block.length - 4);
        final storedCrc = ByteData.sublistView(block, block.length - 4)
            .getUint32(0, Endian.little);

        if (BinaryEncoder.calculateCRC32(payload) != storedCrc) {
          print(
              "Zeytin Warning: Corrupted data detected (Reading canceled): $bId:$t");
          return null;
        }
        blockReader = ByteData.sublistView(payload);
      } else if (magicByte == BinaryEncoder.magicByteV1) {
        blockReader = ByteData.sublistView(block);
      } else {
        print("Zeytin Warning: Unknown magic byte ($magicByte) for $bId:$t");
        return null;
      }
      int offset = 1;
      final boxIdLen = blockReader.getUint32(offset, Endian.little);
      offset += 4 + boxIdLen;
      final tagLen = blockReader.getUint32(offset, Endian.little);
      offset += 4 + tagLen;
      final dataLen = blockReader.getUint32(offset, Endian.little);
      offset += 4;
      if (dataLen == 0) return null;
      final data = BinaryEncoder.decodeValue(
        block.sublist(offset, offset + dataLen),
      );
      _cache.put(key, data);
      return data;
    } catch (e) {
      print("Read error: $e");
      return null;
    }
  }

  void _scheduleFlush() {
    if (_isFlushing) return;
    _flushTimer?.cancel();
    if (_writeBuffer.length >= _flushCountThreshold) {
      _isFlushing = true;
      _flushBuffer();
    } else {
      _flushTimer = Timer(_flushTimeThreshold, () {
        if (_isFlushing) return;
        _isFlushing = true;
        _flushBuffer();
      });
    }
  }

  Future<void> _flushCore() async {
    if (_writeBuffer.isEmpty) return;
    _isFlushing = true;
    final pendingWrites = Map<String, Map<String, dynamic>?>.from(_writeBuffer);
    _writeBuffer.clear();

    try {
      _writer ??= await _dataFile.open(mode: FileMode.append);
      final txId = DateTime.now().microsecondsSinceEpoch.toString();
      final startBytes = BinaryEncoder.encode(
          '__SYS__', 'TX_START_$txId', {'count': pendingWrites.length});
      var currentOffset = await _writer!.length();
      await _writer!.writeFrom(startBytes);
      currentOffset += startBytes.length;
      final List<Map<String, dynamic>> tempUpdates = [];

      for (var entry in pendingWrites.entries) {
        final colonPos = entry.key.indexOf(':');
        final bId = entry.key.substring(0, colonPos);
        final tag = entry.key.substring(colonPos + 1);
        final value = entry.value;
        final bytes = BinaryEncoder.encode(bId, tag, value);
        await _writer!.writeFrom(bytes);

        tempUpdates.add({
          'boxId': bId,
          'tag': tag,
          'off': currentOffset,
          'len': bytes.length,
          'val': value
        });
        currentOffset += bytes.length;
        _dirtyCount++;
        _compactCounter++;
      }
      final commitBytes =
          BinaryEncoder.encode('__SYS__', 'TX_COMMIT_$txId', null);
      await _writer!.writeFrom(commitBytes);
      await _writer!.flush();

      for (var u in tempUpdates) {
        final bId = u['boxId'] as String;
        final tag = u['tag'] as String;
        final val = u['val'];

        if (val == null) {
          _index._index[bId]?.remove(tag);
          if (_index._index[bId]?.isEmpty ?? false) _index._index.remove(bId);
        } else {
          _index.update(bId, tag, u['off'], u['len']);
        }
      }
      if (_dirtyCount >= _saveThreshold && !_isSavingIndex) _autoSave();
      if (_compactCounter >= _compactThreshold && !_isCompacting) {
        _runAutoCompact();
      }
    } catch (e, stackTrace) {
      print("Zeytin Engine (Flush) Error: $e\n$stackTrace");
    } finally {
      _isFlushing = false;
    }
  }

  Future<void> _flushBuffer() => _synchronized(() async {
        await _flushCore();
        if (_writeBuffer.isNotEmpty) {
          _scheduleFlush();
        }
      });
  Future<void> _rebuildSearchIndex() async {
    for (var bId in _index._index.keys) {
      final boxData = _index.getBox(bId);
      if (boxData == null) continue;
      for (var tag in boxData.keys) {
        final data = await _readInternal(bId, tag);
        if (data != null) _updateInternalIndex(bId, tag, data);
      }
    }
  }

  void _updateInternalIndex(String bId, String tag, Map<String, dynamic> data) {
    data.forEach((field, value) {
      if (value is String) {
        _fieldIndex[bId] ??= {};
        _fieldIndex[bId]![field] ??= {};
        _fieldIndex[bId]![field]![value] ??= {};
        _fieldIndex[bId]![field]![value]!.add(tag);
      }
    });
  }

  void _removeFromInternalIndex(
    String bId,
    String tag,
    Map<String, dynamic> data,
  ) {
    data.forEach((field, value) {
      if (value is String) {
        _fieldIndex[bId]?[field]?[value]?.remove(tag);
        if (_fieldIndex[bId]?[field]?[value]?.isEmpty ?? false) {
          _fieldIndex[bId]?[field]?.remove(value);
        }
        if (_fieldIndex[bId]?[field]?.isEmpty ?? false) {
          _fieldIndex[bId]?.remove(field);
        }
      }
    });
    if (_fieldIndex[bId]?.isEmpty ?? false) _fieldIndex.remove(bId);
  }

  Future<List<Map<String, dynamic>>> query(
    String bId,
    String field,
    String prefix,
  ) async {
    return _synchronized(() async {
      final List<Map<String, dynamic>> results = [];
      final fieldIdx = _fieldIndex[bId]?[field];
      if (fieldIdx == null) return results;
      for (var entry in fieldIdx.entries) {
        if (entry.key.startsWith(prefix)) {
          for (var tag in entry.value) {
            final data = await _readInternal(bId, tag);
            if (data != null) results.add(data);
          }
        }
      }
      return results;
    });
  }

  Future<void> _repair() async {
    final int actual = await _dataFile.length();
    final int last = _index.getMaxIndexedOffset();
    if (actual <= last) return;

    final raf = await _dataFile.open(mode: FileMode.read);
    await raf.setPosition(last);
    int pos = last;
    bool inTx = false;
    String currentTx = "";
    List<Map<String, dynamic>> txBuffer = [];

    while (pos < actual) {
      try {
        await raf.setPosition(pos);
        final magicList = await raf.read(1);
        if (magicList.isEmpty) break;

        final magic = magicList[0];
        // Sadece V1 veya V2 ise işlem yap, yoksa bozuk byte'ı atla
        if (magic != BinaryEncoder.magicByteV1 &&
            magic != BinaryEncoder.magicByteV2) {
          pos++;
          continue;
        }

        final bLenBytes = await raf.read(4);
        if (bLenBytes.length < 4) {
          pos++;
          continue;
        }
        final bLen =
            ByteData.sublistView(bLenBytes).getUint32(0, Endian.little);
        if (bLen <= 0 || bLen > 1024) {
          pos++;
          continue;
        }

        final boxIdBytes = await raf.read(bLen);
        final boxId = utf8.decode(boxIdBytes);

        final tLenBytes = await raf.read(4);
        final tLen =
            ByteData.sublistView(tLenBytes).getUint32(0, Endian.little);
        if (tLen <= 0 || tLen > 1024) {
          pos++;
          continue;
        }

        final tagBytes = await raf.read(tLen);
        final tag = utf8.decode(tagBytes);

        final dLenBytes = await raf.read(4);
        final dLen =
            ByteData.sublistView(dLenBytes).getUint32(0, Endian.little);

        // Payload (Verinin CRC'siz kısmı) hesaplanıyor
        final payloadLength = 1 + 4 + bLen + 4 + tLen + 4 + dLen;

        // EĞER V2 İSE 4 BYTE CRC EKLENİYOR
        final totalLength = (magic == BinaryEncoder.magicByteV2)
            ? payloadLength + 4
            : payloadLength;

        if (pos + totalLength > actual) {
          pos++;
          continue;
        }

        await raf.setPosition(pos);
        final block = await raf.read(totalLength);
        final payload = block.sublist(0, payloadLength);

        // V2 İÇİN CRC DOĞRULAMASI
        if (magic == BinaryEncoder.magicByteV2) {
          final storedCrc = ByteData.sublistView(block, payloadLength)
              .getUint32(0, Endian.little);
          if (BinaryEncoder.calculateCRC32(payload) != storedCrc) {
            print(
                "Zeytin Repair: Bad record skipped (CRC Error) [$boxId:$tag]");
            pos++;
            continue;
          }
        }

        Map<String, dynamic>? data;
        if (dLen > 0) {
          final dataBytes =
              payload.sublist(payloadLength - dLen, payloadLength);
          data = BinaryEncoder.decodeValue(dataBytes);
        }

        if (boxId == '__SYS__') {
          if (tag.startsWith('TX_START_')) {
            inTx = true;
            currentTx = tag.replaceFirst('TX_START_', '');
            txBuffer.clear();
          } else if (tag.startsWith('TX_COMMIT_')) {
            final commitTx = tag.replaceFirst('TX_COMMIT_', '');
            if (inTx && currentTx == commitTx) {
              for (var item in txBuffer) {
                if (item['data'] == null) {
                  _index._index[item['boxId']]?.remove(item['tag']);
                  if (_index._index[item['boxId']]?.isEmpty ?? false) {
                    _index._index.remove(item['boxId']);
                  }
                } else {
                  _index.update(
                      item['boxId'], item['tag'], item['pos'], item['len']);
                }
              }
            }
            inTx = false;
            txBuffer.clear();
          }
        } else {
          if (inTx) {
            txBuffer.add({
              'boxId': boxId,
              'tag': tag,
              'pos': pos,
              'len': totalLength,
              'data': data
            });
          } else {
            if (data == null) {
              _index._index[boxId]?.remove(tag);
              if (_index._index[boxId]?.isEmpty ?? false) {
                _index._index.remove(boxId);
              }
            } else {
              _index.update(boxId, tag, pos, totalLength);
            }
          }
        }

        pos += totalLength;
      } catch (e) {
        pos++;
      }
    }
    await raf.close();
    await _index.save();
  }

  Future<void> write(String bId, String t, Map<String, dynamic> v,
          {bool sync = false}) =>
      _synchronized(() async {
        final oldData = await _readInternal(bId, t);
        if (oldData != null) _removeFromInternalIndex(bId, t, oldData);
        _updateInternalIndex(bId, t, v);
        _cache.put('$bId:$t', v);
        _writeBuffer['$bId:$t'] = v;

        if (sync) {
          _flushTimer?.cancel();
          await _flushCore();
        } else {
          _scheduleFlush();
        }
      });

  Future<bool> putCAS(String bId, String t, Map<String, dynamic> v,
          String casField, dynamic expectedValue,
          {bool sync = false}) =>
      _synchronized(() async {
        final oldData = await _readInternal(bId, t);
        if (oldData?[casField] != expectedValue) return false;

        if (oldData != null) _removeFromInternalIndex(bId, t, oldData);
        _updateInternalIndex(bId, t, v);
        _cache.put('$bId:$t', v);
        _writeBuffer['$bId:$t'] = v;

        if (sync) {
          _flushTimer?.cancel();
          await _flushCore();
        } else {
          _scheduleFlush();
        }
        return true;
      });
  Future<void> batch(String bId, Map<String, Map<String, dynamic>> entries) =>
      _synchronized(() async {
        _writer ??= await _dataFile.open(mode: FileMode.append);
        final txId = DateTime.now().microsecondsSinceEpoch.toString();
        final startBytes = BinaryEncoder.encode(
            '__SYS__', 'TX_START_$txId', {'count': entries.length});
        var off = await _writer!.length();
        await _writer!.writeFrom(startBytes);
        off += startBytes.length;
        final List<Map<String, dynamic>> tempUpdates = [];
        for (var entry in entries.entries) {
          final oldData = await _readInternal(bId, entry.key);
          if (oldData != null) {
            _removeFromInternalIndex(bId, entry.key, oldData);
          }
          final bytes = BinaryEncoder.encode(bId, entry.key, entry.value);
          await _writer!.writeFrom(bytes);
          tempUpdates.add({
            'tag': entry.key,
            'off': off,
            'len': bytes.length,
            'val': entry.value
          });
          off += bytes.length;
          _dirtyCount++;
          _compactCounter++;
        }
        final commitBytes =
            BinaryEncoder.encode('__SYS__', 'TX_COMMIT_$txId', null);
        await _writer!.writeFrom(commitBytes);
        await _writer!.flush();
        for (var u in tempUpdates) {
          _index.update(bId, u['tag'], u['off'], u['len']);
          _updateInternalIndex(bId, u['tag'], u['val']);
          _cache.put('$bId:${u['tag']}', u['val']);
        }

        if (_dirtyCount >= _saveThreshold && !_isSavingIndex) _autoSave();
        if (_compactCounter >= _compactThreshold && !_isCompacting) {
          _runAutoCompact();
        }
      });
  Future<void> removeTag(String bId, String t, {bool sync = false}) =>
      _synchronized(() async {
        final oldData = await _readInternal(bId, t);
        if (oldData != null) _removeFromInternalIndex(bId, t, oldData);
        _cache.remove('$bId:$t');
        _writeBuffer['$bId:$t'] = null;

        if (sync) {
          _flushTimer?.cancel();
          await _flushCore();
        } else {
          _scheduleFlush();
        }
      });
  Future<void> removeBox(String bId, {bool sync = false}) =>
      _synchronized(() async {
        final box = _index.getBox(bId);
        if (box == null) return;
        for (var t in box.keys.toList()) {
          final oldData = await _readInternal(bId, t);
          if (oldData != null) _removeFromInternalIndex(bId, t, oldData);
          _cache.remove('$bId:$t');
          _writeBuffer['$bId:$t'] = null;
        }
        _index._index.remove(bId);
        _fieldIndex.remove(bId);

        if (sync) {
          _flushTimer?.cancel();
          await _flushCore();
        } else {
          _scheduleFlush();
        }
      });
  void _runAutoCompact() {
    _isCompacting = true;
    _compactCounter = 0;
    compact()
        .then((_) => _isCompacting = false)
        .catchError((_) => _isCompacting = false);
  }

  void _autoSave() {
    _isSavingIndex = true;
    _dirtyCount = 0;
    _index
        .save()
        .then((_) => _isSavingIndex = false)
        .catchError((_) => _isSavingIndex = false);
  }

  Future<Map<String, dynamic>?> read(String bId, String t) =>
      _synchronized(() => _readInternal(bId, t));
  Future<Map<String, Map<String, dynamic>>> readBox(String bId) =>
      _synchronized(() async {
        final res = <String, Map<String, dynamic>>{};
        final tags = _index.getBox(bId)?.keys.toList() ?? [];
        for (var t in tags) {
          final data = await _readInternal(bId, t);
          if (data != null) res[t] = data;
        }
        return res;
      });
  Future<List<String>> getAllBoxes() => _synchronized(() async {
        return _index._index.keys.where((k) => k != '__SYS__').toList();
      });
  Future<void> compact() => _synchronized(() async {
        final tempDatPath = '$path/${id}_temp.dat';
        final tempIdxPath = '$path/${id}_temp.idx';
        final tempFile = File(tempDatPath);
        final newIndex = PersistentIndex(tempIdxPath);

        IOSink? sink;
        bool preparationSuccess = false;
        try {
          sink = tempFile.openWrite();
          int currentOffset = 0;

          for (var bId in _index._index.keys.toList()) {
            for (var tag in (_index._index[bId]?.keys.toList() ?? [])) {
              final data = await _readInternal(bId, tag);
              if (data != null) {
                final bytes = BinaryEncoder.encode(bId, tag, data);
                sink.add(bytes);
                newIndex.update(bId, tag, currentOffset, bytes.length);
                currentOffset += bytes.length;
              }
            }
          }
          await sink.flush();
          await sink.close();
          sink = null;
          await newIndex.save();
          preparationSuccess = true;
        } catch (e) {
          print("Zeytin Engine (Compact) Preparation Error: $e");
        } finally {
          if (sink != null) await sink.close();
        }
        if (!preparationSuccess) {
          if (await tempFile.exists()) await tempFile.delete();
          if (await File(tempIdxPath).exists()) {
            await File(tempIdxPath).delete();
          }
          return;
        }
        final oldDataFile = _dataFile;
        final oldIdxFile = File(_index._file.path);
        final backupDataFile = File('$path/${id}_bak.dat');
        final backupIdxFile = File('$path/${id}_bak.idx');
        try {
          await _reader?.close();
          await _writer?.close();
          _reader = _writer = null;
          if (await backupDataFile.exists()) await backupDataFile.delete();
          if (await backupIdxFile.exists()) await backupIdxFile.delete();
          if (await oldDataFile.exists()) {
            await oldDataFile.rename(backupDataFile.path);
          }
          if (await oldIdxFile.exists()) {
            await oldIdxFile.rename(backupIdxFile.path);
          }
          await tempFile.rename(oldDataFile.path);
          await File(tempIdxPath).rename(oldIdxFile.path);
          _index._index = newIndex._index;
          if (await backupDataFile.exists()) await backupDataFile.delete();
          if (await backupIdxFile.exists()) await backupIdxFile.delete();
        } catch (e) {
          print("Zeytin Engine (Compact) Critical File Error: $e");
          try {
            if (await backupDataFile.exists() &&
                !(await oldDataFile.exists())) {
              await backupDataFile.rename(oldDataFile.path);
            }
            if (await backupIdxFile.exists() && !(await oldIdxFile.exists())) {
              await backupIdxFile.rename(oldIdxFile.path);
            }
          } catch (restoreError) {
            print("Zeytin Engine (Compact) Recovery Failed: $restoreError");
          }
        } finally {
          try {
            _writer = await _dataFile.open(mode: FileMode.append);
          } catch (e) {
            print("Zeytin Engine (Compact) Restart Error: $e");
          }
        }
      });
  Future<void> close() async => _synchronized(() async {
        _flushTimer?.cancel();
        if (_writeBuffer.isNotEmpty) {
          await _flushBuffer();
        }
        await _index.save();
        await _reader?.close();
        await _writer?.close();
        _reader = _writer = null;
      });
}

class TruckIsolate {
  late Truck _truck;
  Future<void> init(String id, String path) async {
    _truck = Truck(id, path);
    await _truck.initialize();
  }

  Future<void> write(String bId, String t, Map<String, dynamic> v, bool sync) =>
      _truck.write(bId, t, v, sync: sync);
  Future<bool> putCAS(String bId, String t, Map<String, dynamic> v, String casF,
          dynamic expV, bool sync) =>
      _truck.putCAS(bId, t, v, casF, expV, sync: sync);
  Future<Map<String, dynamic>?> read(String bId, String t) =>
      _truck.read(bId, t);
  Future<void> batch(String bId, Map<String, Map<String, dynamic>> e) =>
      _truck.batch(bId, e);
  Future<Map<String, Map<String, dynamic>>> readBox(String bId) =>
      _truck.readBox(bId);
  Future<List<Map<String, dynamic>>> query(String bId, String f, String p) =>
      _truck.query(bId, f, p);
  Future<List<String>> getAllBoxes() => _truck.getAllBoxes();
  Future<void> compact() => _truck.compact();
  Future<void> close() => _truck.close();
  Future<void> removeTag(String bId, String t, bool sync) =>
      _truck.removeTag(bId, t, sync: sync);
  Future<void> removeBox(String bId, bool sync) =>
      _truck.removeBox(bId, sync: sync);
  Future<bool> contains(String bId, String t) async =>
      (await _truck.read(bId, t)) != null;
}

class TruckProxy {
  final String id, path;
  late SendPort _sendPort;
  final Map<int, Completer<dynamic>> _completers = {};
  int _messageId = 0;
  final ReceivePort _receivePort = ReceivePort();
  Isolate? _isolate;
  TruckProxy(this.id, this.path);
  Future<void> initialize() async {
    final completer = Completer<void>();
    _isolate = await Isolate.spawn(_startTruckIsolate, _receivePort.sendPort);
    _receivePort.listen((message) {
      if (message is SendPort) {
        _sendPort = message;
        _sendCommand('init', {'id': id, 'path': path}).then((_) {
          if (!completer.isCompleted) completer.complete();
        }).catchError((e) {
          if (!completer.isCompleted) completer.completeError(e);
        });
      } else if (message is Map) {
        final msgId = message['id'] as int, msgCompleter = _completers[msgId];
        if (msgCompleter != null) {
          if (message.containsKey('result')) {
            msgCompleter.complete(message['result']);
          } else {
            msgCompleter.completeError(Exception(message['error']));
          }
          _completers.remove(msgId);
        }
      }
    });
    return completer.future;
  }

  Future<void> writeSync(String bId, String t, Map<String, dynamic> v) =>
      _sendCommand('write', {'boxId': bId, 'tag': t, 'value': v, 'sync': true});

  Future<void> removeTagSync(String bId, String t) =>
      _sendCommand('removeTag', {'boxId': bId, 'tag': t, 'sync': true});

  Future<void> removeBoxSync(String bId) =>
      _sendCommand('removeBox', {'boxId': bId, 'sync': true});
  void writeFast(String bId, String t, Map<String, dynamic> v) {
    _sendPort.send({
      'command': 'write',
      'params': {'boxId': bId, 'tag': t, 'value': v},
      'id': -1
    });
  }

  void removeTagFast(String bId, String t) {
    _sendPort.send({
      'command': 'removeTag',
      'params': {'boxId': bId, 'tag': t},
      'id': -1
    });
  }

  void removeBoxFast(String bId) {
    _sendPort.send({
      'command': 'removeBox',
      'params': {'boxId': bId},
      'id': -1
    });
  }

  static void _startTruckIsolate(SendPort sendPort) {
    final receivePort = ReceivePort();
    sendPort.send(receivePort.sendPort);
    final truckIsolate = TruckIsolate();
    receivePort.listen((message) async {
      if (message is Map) {
        final command = message['command'] as String,
            params = message['params'] as Map<String, dynamic>,
            id = message['id'] as int;
        try {
          dynamic res;
          switch (command) {
            case 'init':
              await truckIsolate.init(params['id'], params['path']);
              break;
            case 'write':
              await truckIsolate.write(params['boxId'], params['tag'],
                  params['value'], params['sync'] ?? false);
              break;
            case 'putCAS':
              res = await truckIsolate.putCAS(
                  params['boxId'],
                  params['tag'],
                  params['value'],
                  params['casField'],
                  params['expectedValue'],
                  params['sync'] ?? false);
              break;
            case 'read':
              res = await truckIsolate.read(params['boxId'], params['tag']);
              break;
            case 'getAllBoxes':
              res = await truckIsolate.getAllBoxes();
              break;
            case 'batch':
              await truckIsolate.batch(params['boxId'], params['entries']);
              break;
            case 'readBox':
              res = await truckIsolate.readBox(params['boxId']);
              break;
            case 'query':
              res = await truckIsolate.query(
                params['boxId'],
                params['field'],
                params['prefix'],
              );
              break;
            case 'removeTag':
              await truckIsolate.removeTag(
                  params['boxId'], params['tag'], params['sync'] ?? false);
              break;
            case 'removeBox':
              await truckIsolate.removeBox(
                  params['boxId'], params['sync'] ?? false);
              break;
            case 'compact':
              await truckIsolate.compact();
              break;
            case 'close':
              await truckIsolate.close();
              break;
            case 'contains':
              res = await truckIsolate.contains(params['boxId'], params['tag']);
              break;
          }
          if (id != -1) {
            sendPort.send({'id': id, 'result': res});
          }
          if (command == 'close') receivePort.close();
        } catch (e, stackTrace) {
          print("ISOLATE FATAL ERROR: $e\n$stackTrace");
          if (id != -1) {
            sendPort.send({'id': id, 'error': e.toString()});
          }

          if (command == 'close') receivePort.close();
        }
      }
    });
  }

  Future<bool> putCAS(String bId, String t, Map<String, dynamic> v, String casF,
          dynamic expV,
          {bool sync = false}) async =>
      await _sendCommand('putCAS', {
        'boxId': bId,
        'tag': t,
        'value': v,
        'casField': casF,
        'expectedValue': expV,
        'sync': sync
      });
  Future<List<String>> getAllBoxes() async =>
      List<String>.from(await _sendCommand('getAllBoxes', {}));
  Future<dynamic> _sendCommand(String command, Map<String, dynamic> params) {
    final id = _messageId++, completer = Completer<dynamic>();
    _completers[id] = completer;
    _sendPort.send({'command': command, 'params': params, 'id': id});
    return completer.future.timeout(
      const Duration(seconds: 30),
      onTimeout: () {
        _completers.remove(id);
        throw TimeoutException('Command $command timed out');
      },
    );
  }

  Future<void> write(String bId, String t, Map<String, dynamic> v) =>
      _sendCommand('write', {'boxId': bId, 'tag': t, 'value': v});
  Future<Map<String, dynamic>?> read(String bId, String t) async =>
      await _sendCommand('read', {'boxId': bId, 'tag': t});
  Future<void> batch(String bId, Map<String, Map<String, dynamic>> e) =>
      _sendCommand('batch', {'boxId': bId, 'entries': e});
  Future<Map<String, Map<String, dynamic>>> readBox(String bId) async =>
      Map<String, Map<String, dynamic>>.from(
        await _sendCommand('readBox', {'boxId': bId}),
      );
  Future<List<Map<String, dynamic>>> query(
    String bId,
    String f,
    String p,
  ) async =>
      List<Map<String, dynamic>>.from(
        await _sendCommand('query', {'boxId': bId, 'field': f, 'prefix': p}),
      );
  Future<void> removeTag(String bId, String t) =>
      _sendCommand('removeTag', {'boxId': bId, 'tag': t});
  Future<void> removeBox(String bId) =>
      _sendCommand('removeBox', {'boxId': bId});
  Future<void> compact() => _sendCommand('compact', {});
  Future<void> close() async {
    try {
      await _sendCommand('close', {});
    } catch (_) {}
    _receivePort.close();
    _isolate?.kill();
  }

  Future<bool> contains(String bId, String t) async =>
      await _sendCommand('contains', {'boxId': bId, 'tag': t});
}

class Zeytin {
  static const int _maxActiveTrucks = 50;
  final String rootPath;
  final LRUCache<String, Map<String, dynamic>> _memoryCache;
  final Map<String, TruckProxy> _activeTrucks = {};
  final List<String> _truckAccessOrder = [];
  final StreamController<Map<String, dynamic>> _changeController =
      StreamController<Map<String, dynamic>>.broadcast();
  Zeytin(this.rootPath, {int cacheSize = 50000})
      : _memoryCache = LRUCache(cacheSize) {
    Directory(rootPath).createSync(recursive: true);
  }
  Stream<Map<String, dynamic>> get changes => _changeController.stream;
  Future<TruckProxy> _resolveTruck({required String truckId}) async {
    if (_activeTrucks.containsKey(truckId)) {
      _truckAccessOrder.remove(truckId);
      _truckAccessOrder.add(truckId);
      return _activeTrucks[truckId]!;
    }
    if (_activeTrucks.length >= _maxActiveTrucks) {
      final oldest = _truckAccessOrder.removeAt(0);
      await _activeTrucks[oldest]!.close();
      _activeTrucks.remove(oldest);
    }
    final truck = TruckProxy(truckId, rootPath);
    await truck.initialize();
    _activeTrucks[truckId] = truck;
    _truckAccessOrder.add(truckId);
    return truck;
  }

  String _cacheKey(String tId, String bId, String t) => '$tId:$bId:$t';

  Future<void> put({
    required String truckId,
    required String boxId,
    required String tag,
    required Map<String, dynamic> value,
    bool sync = false,
  }) async {
    bool isUpdate = _memoryCache.contains(_cacheKey(truckId, boxId, tag));
    final truck = await _resolveTruck(truckId: truckId);

    _memoryCache.put(_cacheKey(truckId, boxId, tag), value);

    if (sync) {
      await truck.writeSync(boxId, tag, value);
    } else {
      truck.writeFast(boxId, tag, value);
    }

    _changeController.add({
      "truckId": truckId,
      "boxId": boxId,
      "tag": tag,
      "op": isUpdate ? "UPDATE" : "PUT",
      "value": value,
    });
  }

  Future<bool> putCAS({
    required String truckId,
    required String boxId,
    required String tag,
    required Map<String, dynamic> value,
    required String casField,
    required dynamic expectedValue,
    bool sync = false,
  }) async {
    final truck = await _resolveTruck(truckId: truckId);
    final success = await truck
        .putCAS(boxId, tag, value, casField, expectedValue, sync: sync);
    if (success) {
      _memoryCache.put(_cacheKey(truckId, boxId, tag), value);
      _changeController.add({
        "truckId": truckId,
        "boxId": boxId,
        "tag": tag,
        "op": "CAS_UPDATE",
        "value": value,
      });
    }
    return success;
  }

  Future<void> compactTruck({required String truckId}) async {
    if (_activeTrucks.containsKey(truckId)) {
      final truck = _activeTrucks[truckId]!;
      await truck.compact();
      _memoryCache.clear();
    }
  }

  Future<void> deleteTruck(String truckId) async {
    if (_activeTrucks.containsKey(truckId)) {
      await _activeTrucks[truckId]!.close();
      _activeTrucks.remove(truckId);
    }
    final dataFile = File('$rootPath/$truckId.dat');
    final indexFile = File('$rootPath/$truckId.idx');
    if (await dataFile.exists()) await dataFile.delete();
    if (await indexFile.exists()) await indexFile.delete();
  }

  Future<void> deleteAll() async {
    for (var truck in _activeTrucks.values) {
      await truck.close();
    }
    _activeTrucks.clear();
    _memoryCache.clear();
    final dir = Directory(rootPath);
    if (await dir.exists()) {
      try {
        await dir.delete(recursive: true);
        await dir.create(recursive: true);
      } catch (_) {}
    }
  }

  Future<bool> existsTruck({required String truckId}) async {
    final dataFile = File('$rootPath/$truckId.dat');
    return await dataFile.exists();
  }

  Future<void> putBatch({
    required String truckId,
    required String boxId,
    required Map<String, Map<String, dynamic>> entries,
  }) async {
    final truck = await _resolveTruck(truckId: truckId);
    await truck.batch(boxId, entries);
    for (var entry in entries.entries) {
      _memoryCache.put(_cacheKey(truckId, boxId, entry.key), entry.value);
    }
    _changeController.add({
      "truckId": truckId,
      "boxId": boxId,
      "op": "BATCH",
      "entries": entries,
    });
  }

  Future<Map<String, dynamic>?> get({
    required String truckId,
    required String boxId,
    required String tag,
  }) async {
    final cKey = _cacheKey(truckId, boxId, tag),
        cached = _memoryCache.get(cKey);
    if (cached != null) return cached;
    final truck = await _resolveTruck(truckId: truckId),
        data = await truck.read(boxId, tag);
    if (data != null) _memoryCache.put(cKey, data);
    return data;
  }

  Future<Map<String, Map<String, dynamic>>> getBox({
    required String truckId,
    required String boxId,
  }) async {
    final truck = await _resolveTruck(truckId: truckId);
    return await truck.readBox(boxId);
  }

  Future<bool> existsBox({
    required String truckId,
    required String boxId,
  }) async {
    final truck = await _resolveTruck(truckId: truckId);
    final data = await truck.readBox(boxId);
    return data.isNotEmpty;
  }

  Future<bool> existsTag({
    required String truckId,
    required String boxId,
    required String tag,
  }) async {
    if (_memoryCache.contains(_cacheKey(truckId, boxId, tag))) return true;
    final truck = await _resolveTruck(truckId: truckId);
    return await truck.contains(boxId, tag);
  }

  Future<bool> contains(String truckId, String boxId, String tag) async =>
      existsTag(truckId: truckId, boxId: boxId, tag: tag);
  Future<void> delete({
    required String truckId,
    required String boxId,
    required String tag,
    bool sync = false,
  }) async {
    final truck = await _resolveTruck(truckId: truckId);
    _memoryCache.remove(_cacheKey(truckId, boxId, tag));

    if (sync) {
      await truck.removeTagSync(boxId, tag);
    } else {
      truck.removeTagFast(boxId, tag);
    }

    _changeController.add({
      "truckId": truckId,
      "boxId": boxId,
      "tag": tag,
      "op": "DELETE",
    });
  }

  Future<void> deleteBox({
    required String truckId,
    required String boxId,
    bool sync = false,
  }) async {
    final truck = await _resolveTruck(truckId: truckId);
    _memoryCache.clear();

    if (sync) {
      await truck.removeBoxSync(boxId);
    } else {
      truck.removeBoxFast(boxId);
    }

    _changeController.add({
      "truckId": truckId,
      "boxId": boxId,
      "op": "DELETE_BOX",
    });
  }

  Future<List<Map<String, dynamic>>> search(
    String truckId,
    String boxId,
    String field,
    String prefix,
  ) async {
    final truck = await _resolveTruck(truckId: truckId);
    return await truck.query(boxId, field, prefix);
  }

  Future<List<Map<String, dynamic>>> filter(
    String truckId,
    String boxId,
    bool Function(Map<String, dynamic>) predicate,
  ) async {
    final boxData = await getBox(truckId: truckId, boxId: boxId);
    return boxData.values.where(predicate).toList();
  }

  Future<void> createTruck({required String truckId}) async {
    await _resolveTruck(truckId: truckId);
  }

  List<String> getAllTruck() {
    return Directory(rootPath)
        .listSync()
        .where((e) => e is File && e.path.endsWith('.dat'))
        .map(
          (e) =>
              e.path.split(Platform.pathSeparator).last.replaceAll('.dat', ''),
        )
        .toList();
  }

  Future<List<String>> getAllBoxes(String truckId) async {
    final truck = await _resolveTruck(truckId: truckId);
    return await truck.getAllBoxes();
  }

  Future<void> close() async {
    for (var truck in _activeTrucks.values) {
      await truck.close();
    }
    _activeTrucks.clear();
    _truckAccessOrder.clear();
    _memoryCache.clear();
    await _changeController.close();
  }
}
