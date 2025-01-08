import 'lower_bounds.dart';
import 'queue.dart';

class _PriorityQueueOptions
 {
  _PriorityQueueOptions(this.priority, { this.run, this.key });

  int priority;

  RunFunction? run;

  dynamic key;
}

class PriorityQueue implements IQueue<RunFunction?, _PriorityQueueOptions>{
  @override
  int get size {
    return this._queue.length;
  }

  List<_PriorityQueueOptions> _queue = <_PriorityQueueOptions>[];
  final _map = <dynamic, _PriorityQueueOptions>{};

  @override
  int enqueueFirst(RunFunction? run, {key}) {
    if (size == 0) {
      enqueue(run, key: key);
      return 0;
    }
    _PriorityQueueOptions element = _PriorityQueueOptions(
      _queue.first.priority,
      run: run,
      key: key,
    );
    _enqueueInternal(element, 0);
    return element.priority;
  }

  @override
  void enqueue(run, { int priority = 0, dynamic key }) {

    _PriorityQueueOptions element = _PriorityQueueOptions(
      priority,
      run: run,
      key: key,
    );

    var index;
    if (size > 0 && _queue[size - 1].priority >= priority) {
      index = size;
    }

    index = lowerBound<_PriorityQueueOptions>(
      _queue,
      element,
      (a, b) => b.priority - a.priority);

    _enqueueInternal(element, index);
  }

  void _enqueueInternal(_PriorityQueueOptions element, int index) {
    var key = element.key;
    if (key != null) {
      if (_map[key] != null) {
        throw Exception('keyed entry already exists');
      }
      _map[key] = element;
    }

    if (index == size) {
      _queue.add(element);
      return;
    }
    _queue.insert(index, element);
  }


  @override
  RunFunction? dequeue({ dynamic key }) {
    _PriorityQueueOptions? item;

    if (key != null) {
      int idx = _map[key] != null ? _queue.indexOf(_map[key]!) : -1;
      item = idx >= 0 ? _queue.removeAt(idx) : null;
    } else {
      item = _queue.isNotEmpty
        ? _queue.removeAt(0)
        : null;
    }

    if (item?.key != null) {
      _map.remove(item?.key);
    }

    return item?.run;
  }

  int indexOf(dynamic key) {
    if (_map.containsKey(key)) {
      return _queue.indexOf(_map[key]!);
    }
    return -1;
  }

  @override
  List<RunFunction?> filter(int priority) {
    return this._queue.where((element) => element.priority == priority).map((e) => e.run) as List<Future<dynamic> Function()?>;
  }

  Iterable get keys => _map.keys;

}
