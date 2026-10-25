import importlib.util, json
spec = importlib.util.spec_from_file_location("nh", "/d/workspaces_261610/akirabot/group_img_saver_svc/napcat_health.py".replace("/d/","D:/"))
nh = importlib.util.module_from_spec(spec); spec.loader.exec_module(nh)
now = 1793000000.0
ok = {"verdict":"ok"}; down = {"verdict":"offline"}
res=[]
def chk(n, c, d=""):
    res.append(c); print(("PASS " if c else "FAIL ")+n+("  "+d if d else ""))

st={}
m,st = nh.decide_alert(down, st, now); chk("第1次掉线不推", m is None, str(st))
m,st = nh.decide_alert(down, st, now+120); chk("第2次掉线不推", m is None, f"streak={st.get('down_streak')}")
m,st = nh.decide_alert(down, st, now+240); chk("第3次掉线推送", m and m["kind"]=="down", str(m and m["title"]))
m,st = nh.decide_alert(down, st, now+360); chk("第4次不重复推", m is None, str(st))
m,st = nh.decide_alert(ok, st, now+480); chk("恢复时推一次", m and m["kind"]=="recover", str(m and m["title"]))
m,st = nh.decide_alert(ok, st, now+600); chk("恢复后不再推", m is None, "")
m,st = nh.decide_alert(down, st, now+3600); chk("新故障重新计数(第1次不推)", m is None, f"alerts={st.get('alerts')}")
m,st = nh.decide_alert(down, st, now+3720); chk("第2次不推", m is None, "")
m,st = nh.decide_alert(down, st, now+3840); chk("第3次触发但今日额度已满→抑制", m and m["kind"]=="down_suppressed", str(m and m["kind"]))
st2={"day":st.get("day"),"alerts":0,"down_streak":0,"alerted_down":False}
m,st2 = nh.decide_alert(down, st2, now); m,st2 = nh.decide_alert(down, st2, now+120); m,st2 = nh.decide_alert(down, st2, now+240)
chk("额度重置后能推", m and m["kind"]=="down", str(m and m["kind"]))
m,st2 = nh.decide_alert(ok, st2, now+300)
chk("恢复推后额度=2", st2.get("alerts")==2, f"alerts={st2.get('alerts')}")
m,st2 = nh.decide_alert(down, st2, now+400); m,st2 = nh.decide_alert(down, st2, now+520); m,st2 = nh.decide_alert(down, st2, now+640)
chk("额度用尽后掉线仅抑制不推送", m is None or m["kind"]=="down_suppressed", str(m and m["kind"]))
print(f"\n== {sum(res)}/{len(res)} passed ==")
raise SystemExit(0 if all(res) else 1)
