import difflib

def test1(string):
    # string = "基于信道质量切换开关:关&同频条件切换开关:关&同频协同切换优化开关:关"
    splits = string.split("&")
    result = []
    for s in splits:
        col = s.split(":")[0]
        result.append(col)
    print(result)

def compare_strings(str1, str2):
   d = difflib.Differ()
   return list(d.compare(list(str1), list(str2)))

def test2(string):
    splits = string.split("  ")
    splits = list(filter(lambda x: x.strip() != "", splits))

    print(splits)
    print(len(splits))

if __name__ == "__main__":
    str1 = "LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=0"
    str2 = "LST NRCELLHOEUTRANMEAGRP:INTERRHOTOEUTRANMEASGRPID=0;"
   #test1("E-UTRAN切换开关:开&E-UTRAN重定向开关:开&语音业务盲模式开关:开&基于覆盖的E-UTRAN最强邻区重定向开关:关&VoNR基于覆盖的切换优先开关:关&重建流程中EPS Fallback变更开关:关")
   # test2("NR小区标识  服务质量等级  流控参数组标识  AM模式PDCP参数组标识  UM模式PDCP参数组标识  RLC模式  DRX参数组标识  异系统切换测量参数组标识  异系统切换至E-UTRAN测量参数组标识  "
   #       "同频切换测量参数组标识  异频切换测量参数组标识  NSA DC配置参数组标识  切换与QoS流程冲突处理策略  负载均衡算法开关                             负载均衡的下行吞吐率门限("
   #       "千比特/秒)  负载均衡的上行吞吐率门限(千比特/秒)  负载均衡的下行缓存数据量(千比特)  负载均衡的上行BSR数据量(千比特)  负载均衡的检查时长(秒)  NSA用户异频切换测量参数组标识  "
   #       "低速用户同频切换测量参数组标识  低速用户异频切换测量参数组标识  RedCap用户DRX参数组标识  业务释放延迟定时器(毫秒)  gNodeB频点优先级组标识  QCI算法参数组标识")
   #res =compare_strings("LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=0", "LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=1")
    dff1 = [diff_str for diff_str in str1 if diff_str not in str2]
    print(dff1)