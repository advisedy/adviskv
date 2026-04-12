阿巴阿巴之前没想过写写日志啥的，所以现在补上吧。



原本的架构是把关于place_table的内容全部都放在了PlaceTableService里面的，但是这样显得太过冗余了。
而且所有的信息都堆在了Service里面，感觉很臃肿，耦合度也很高。
应该再抽象出来一层，把这个操作的内容分类出去。

于是Operation就出现了，同时还会有Plan。
把place_table的部分划分成两部分，就是构造出来我们需要更新的东西，然后去更新它们。

所以我们可以Operatioin的操作规定成: 制作plan，然后更新这个plan。

然后我们会发现那这个Operation也是需要用到Manager的。
那么初始化的话又会传递一堆参数，感觉太丑。

于是，OperatioinFactory出现了。

然后我们稍微的重构一下，让这个Service只拿到Factory，具体的函数，例如place_table就让factory去根据我们
传进来的param去生成出来算子，然后让算子去执行就好了。

然后我们就需要再factory里面写个创建placeTableOp算子的过程了。
并且把所有需要用到的manager都直接全部放到factory里面，然后创建placeTableOp的时候用到哪个我们就把哪个传进这个算子的Deps里。

以上就是大概思路。

之后考虑应该把这个创建算子的部分写成宏，不然每一次都是这种=又臭又长，感觉太丑了。

