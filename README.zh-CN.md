[English](README.md) | **简体中文**

<p align="center"><img src="assets/logo.svg" alt="pushbus" width="200" height="40"></p>

# pushbus

面向 Python 的推送式路由总线：值沿命名路径到来，按正则捕获键配对，交给普通函数处理，再通过输出模板重新推送出去。

**它把一堆互不相干的回调变成一张数据流图——先把输入缓存到配对成功，再对每一对调用一次你的函数，并把结果继续路由下去；不需要消息中间件，不需要队列服务，不引入框架，也没有任何依赖。**

`pushbus` 只有一个模块。你只需要给普通的 Python 函数加装饰器；缓存、配对、定时器和计数都由总线负责。线程和异步仍归你管：总线自己从不启动调度器。

## 快速开始

```sh
git clone https://github.com/ZisIsNotZis/pushbus
cd pushbus
python -m pytest
```

安装到此为止：库就是 `pushbus.py`，测试期唯一依赖是 `pytest`。需要 Python 3.10 或更新版本（总线会检查 `types.UnionType` 来识别 `T | None` 端口）。

然后：

```python
from pushbus import PushBus

bus = PushBus()

@bus.subscribe(
    a=r"temp/(?P<room>\w+)",
    b=r"hum/(?P<room>\w+)",
    _outputs="comfort/{room}",
)
def comfort(temp: float, hum: float):
    return temp + hum

@bus.subscribe(_inputs=r"comfort/(?P<room>\w+)", _outputs="log")
def show(value: float):
    print(f"comfort = {value}")

bus.publish(21.5, "temp/kitchen")   # 先缓存：还没有配对对象
bus.publish(40.0, "hum/kitchen")    # 同一个房间 -> 触发配对 -> show(61.5)
```

拥有同名捕获组（这里是 `room`）的输入共同构成配对键，所以 `temp/kitchen` 只会和 `hum/kitchen` 配对；只出现在单个输入上的捕获（同样是 `room`）依然可以在输出模板里使用。

## 当前能力

- **路径路由** —— 每条输入路径都是全匹配正则，命名捕获按值逐条提取。
- **按键配对** —— 所有输入共享的捕获组定义配对键；每个具体键各有一份缓存，房间、用户、设备之间不会串味。
- **仅提取用的捕获** —— 只出现在某个输入上的捕获组不参与配对键，但可用于 `"comfort/{room}"` 这类输出模板。
- **笛卡尔积兜底** —— 若没有任何共享捕获组，则只用一个键，并对缓存值做笛卡尔组合后逐次发出。
- **三种输入写法** —— 位置参数路径、`_inputs=` 传单条路径或序列，或 `_inputs=` 传“端口名 → 路径”的映射。
- **多个输出** —— 返回元组时按位置映射到各个输出模板；只有一个输出时直接返回普通值。
- **可选端口** —— 标注为 `Optional[T]` 或 `T | None` 的参数允许缺席并收到 `None`；至少要有一个必需输入存在才会触发调用。
- **消费或保留** —— `_consume=True`（默认）在配对成功后移除用掉的值；`_consume=False` 把它们当作最新值保留，但仍需一次新的更新才会再次发出。
- **去抖与硬上限** —— `_grace_period` 在每次更新时重置静默定时器；`_max_grace_period` 限制总等待时间并强制发出一次。定时器带代数校验，过期回调绝不会重复触发。
- **错误隔离** —— 你的函数抛出的异常会被记录并计数，其值仍留在缓存中，也不影响其他路由；内部错误（例如错误的输出模板）会抛给调用方。
- **来源追踪** —— `current_provenance()` 返回产生本次调用的各输入路径。
- **统计信息** —— `bus.statistics()` 提供快照：每个节点的调用数、成功数、失败数、最近错误与耗时，以及每个“端口 × 键”的边调用次数和时间戳。
- **安全日志** —— 值经 `safe_repr()` 记录：截断到 100 个字符，`__repr__` 有问题的对象也不会中断分发。
- **注销** —— 每次订阅都会得到一个 `Disposable`，用 `fn.dispose()` / `fn.subscription` 注销并取消其定时器。
- **线程安全** —— 每个节点持有可重入锁；`publish` 取一次注册快照，因此发布过程中新注册的路由从下一次发布开始生效。

## 架构

整个库就是围绕“每个订阅一台状态机”的单一分发器。

`PushBus.publish(value, path)` 先记录值，对已注册节点做一次快照，然后按注册顺序把值交给每个节点。节点用各自每个输入端口去匹配路径；匹配成功就把值按具体配对键追加到该端口的缓存里。当所有必需端口都有值，节点进入就绪状态：启动静默定时器，静默到期时（`_grace_period` 为 `0` 则立即）取出缓存中的全部组合，对每个组合调用一次用户函数，更新计数，再把每个返回值按输出模板重新发布——这次发布重新进入 `publish`，于是路由可以串成链。

```
pushbus.py          整个库
  PushBus           注册、发布、按节点分发、统计
  _Node / _Port     一次订阅及其编译后的输入路径
  _KeyState         每个键的缓存、代数计数、静默与最长定时器
  _Item             一条缓存值：值、路径、捕获、来源、序号
  Disposable        幂等的注销句柄
test_pushbus.py     针对契约的行为测试
PLAN.md             契约：配对键、静默期、来源、统计
docs/project-status.md  里程碑状态与证据
```

静默期为零时是同步的深度优先遍历（`a → b → c` 全部走完，才会处理 `a` 的下一条路由）；静默期非零时，发出动作移到守护 `threading.Timer` 线程上。两种情况下的缓存状态都由节点级锁保护，并且设计上假定外部调用方负责执行与持久化——它不是队列，也没有拉取接口。

## 如何开发

```sh
python -m pytest                          # full suite
python -m pytest -k grace                 # single behaviour
python -m pytest --log-cli-level=INFO      # watch routing decisions
```

仓库里没有打包元数据、linter 配置或类型检查配置；现有的检查就是测试套件。测试用到了 `pytest` 的特性（`caplog`、`pytest.raises`），新测试请保持同样的风格。

## 如何贡献

见 [CONTRIBUTING.md](CONTRIBUTING.md)。简而言之：先开 issue 描述想要的行为，把改动限制在 [`PLAN.md`](PLAN.md) 的契约之内，并补一个在修复前会失败的测试。

## 路线图

**已实现** —— [`PLAN.md`](PLAN.md) 中的完整契约：基于共享正则捕获键的多输入配对、仅提取用的捕获、无键时的笛卡尔组合、可选端口、消费与保留模式、静默期与最大等待期、元组输出、按键来源追踪、用户错误隔离、有界日志、动态统计、线程安全状态，以及单输入的便捷路径。

**计划中** —— 无。本仓库已于 2026-09-29 作为里程碑关闭；状态与证据记录在 [`docs/project-status.md`](docs/project-status.md)。

未来是刻意收窄的：如果输入或目标发生变化，工作将从 [`PLAN.md`](PLAN.md) 中已写明的范围排除项出发——不做重试策略、不做持久化、不做队列或拉取模型、不做通用运行时类型系统、不做生命周期框架。在那之前，总线仍然小到可以一口气读完，这正是它的目的。

## 许可证

AGPL-3.0，见 [LICENSE.md](LICENSE.md)。版权归 ZisIsNotZis 所有。
