"""Visible UI preparation shared by the independent browser collector."""
import re
import time

PRESETS = (
    {'key': 'game', 'category': '游戏', 'group': r'^(?:预设)?快捷操作\s*[⌃⌄↑↓^]*$', 'preset': 'afizzy竞品游戏'},
    {'key': 'tool', 'category': '工具', 'group': r'^(?:afizzy\s*)?快捷操作\s*[⌃⌄↑↓^]*$', 'preset': 'afizzy竞品'},
)

class PreparationError(RuntimeError):
    pass


def unique_visible(locator, description, optional=False):
    found=[x for x in locator.all() if x.is_visible()]
    if len(found)>1:
        # Responsive layout measurements can have nonzero rects while clipped
        # outside the toolbar. Prefer a unique actual on-screen hit target;
        # never click a hidden measuring copy or choose between two real ones.
        hit=[x for x in found if x.evaluate('''e=>{
          const r=e.getBoundingClientRect(),l=Math.max(0,r.left),t=Math.max(0,r.top),right=Math.min(innerWidth,r.right),bottom=Math.min(innerHeight,r.bottom);
          if(right<=l || bottom<=t)return false;
          const target=document.elementFromPoint((l+right)/2,(t+bottom)/2);
          return !!target && (target===e || e.contains(target));
        }''')]
        if len(hit)==1:return hit[0]
    if len(found)==1:
        return found[0]
    if not found and optional:
        return None
    raise PreparationError(f'{description}可见匹配数为{len(found)}，未猜测点击位置')


def exact(page, text):
    return page.get_by_text(re.compile(r'^\s*'+re.escape(text)+r'\s*$', re.I))


def selected(locator):
    return locator.evaluate('''e=>{
      const label=e.textContent.replace(/\\s/g,'');
      for(let p=e,n=0;p && n<5 && p.textContent.replace(/\\s/g,'')===label;p=p.parentElement,n++){
        for(const a of ['aria-selected','aria-pressed','aria-checked'])if(p.getAttribute(a)==='true')return true;
        if(p.classList.contains('text-primary') && p.classList.contains('border-primary'))return true;
        if([...p.classList].some(c=>/^(active|selected|ant-tabs-tab-active|text-blue-(500|600|700))$/.test(c)))return true;
      }return false;
    }''')


class PresetUI:
    def __init__(self, page, bridge, tab_id, selectors=None, timeout=30):
        self.page,self.bridge,self.tab_id=page,bridge,tab_id
        self.selectors=selectors or {}
        self.timeout=timeout

    def safe(self):
        access=self.bridge.call('access',self.tab_id)
        if access.get('blocked'):
            raise PreparationError(access.get('reason','需要人工登录或验证'))

    def wait(self, observe, description):
        end=time.monotonic()+self.timeout
        while time.monotonic()<end:
            self.safe()
            value=observe()
            if value:
                return value
            self.page.wait_for_timeout(200)
        raise PreparationError(description+'等待超时，未启动下载任务')

    def category(self, name):
        selector=self.selectors.get('category_'+('game' if name=='游戏' else 'tool'))
        def find():
            if selector:return unique_visible(self.page.locator(selector),'分类'+name,True)
            node=unique_visible(self.page.get_by_role('tab',name=name,exact=True),'分类'+name,True)
            return node or unique_visible(exact(self.page,name),'分类'+name,True)
        node=self.wait(find,'分类'+name+'加载')
        if not selected(node):
            self.safe();node.click()
        self.wait(lambda:selected(node),'分类'+name+'选中状态')
        return node

    def date_inputs(self):
        selector=self.selectors.get('date_range','.ant-picker-range')
        root=unique_visible(self.page.locator(selector),'网站日期范围')
        inputs=[x for x in root.locator('input').all() if x.is_visible()]
        if len(inputs)!=2:
            raise PreparationError('日期范围没有唯一的开始/结束输入框，请配置date_range选择器')
        return inputs

    def read_dates(self):
        return [x.input_value().strip().replace('/','-') for x in self.date_inputs()]

    def pick_calendar(self, value):
        year,month,_=map(int,value.split('-'))
        for _ in range(241):
            dropdown=unique_visible(self.page.locator('.ant-picker-dropdown'),'日期面板')
            cell=unique_visible(dropdown.locator(f'.ant-picker-cell-in-view[title="{value}"]:not(.ant-picker-cell-disabled)'),value,True)
            if cell:
                self.safe();cell.click();return
            panels=[x for x in dropdown.locator('.ant-picker-panel').all() if x.is_visible()]
            if not panels:raise PreparationError('无法识别日期日历面板')
            view=panels[0].locator('.ant-picker-header-view').inner_text()
            numbers=re.findall(r'\d+',view)
            if len(numbers)!=2 or len(numbers[0])!=4:
                raise PreparationError('无法核对日历年月：'+view)
            current=int(numbers[0])*12+int(numbers[1]);target=year*12+month
            if current==target:raise PreparationError('目标日期在当前月不可选择')
            panel=panels[0] if target<current else panels[-1]
            button=unique_visible(panel.locator('.ant-picker-header-prev-btn' if target<current else '.ant-picker-header-next-btn'),'日历翻月')
            self.safe();button.click()
        raise PreparationError('日历翻月超过240次预算')

    def set_date(self, value):
        # Always execute AFTER the preset; a preset can replace date state.
        def ready():
            root=unique_visible(self.page.locator(self.selectors.get('date_range','.ant-picker-range')),'网站日期范围',True)
            return root and len([n for n in root.locator('input').all() if n.is_visible()])==2
        self.wait(ready,'预设后的日期控件加载')
        for index in (0,1):
            field=self.date_inputs()[index]
            self.safe()
            if field.get_attribute('readonly') is not None:
                field.click();self.pick_calendar(value)
            else:
                field.fill(value)
                field.press('Tab' if index==0 else 'Enter')
        confirm=unique_visible(self.page.locator('.ant-picker-dropdown').get_by_role('button',name=re.compile(r'^(确定|确 定|OK)$')),'日期确定',True)
        if confirm:self.safe();confirm.click()
        self.wait(lambda:self.read_dates()==[value,value],'指定日期读回')

    def prepare(self, preset, value, evidence):
        self.safe();self.page.screenshot(path=str(evidence/'before.png'),full_page=True)
        self.category(preset['category'])
        before=self.bridge.call('context',self.tab_id)
        try:item=unique_visible(exact(self.page,preset['preset']),'快捷预设'+preset['preset'],True)
        except PreparationError:item=None
        if item is None:
            group_selector=self.selectors.get('group_'+preset['key'])
            group=unique_visible(self.page.locator(group_selector) if group_selector else self.page.get_by_text(re.compile(preset['group'],re.I)),'快捷操作分组')
            self.safe();group.click()
            def menu_item():
                menus=self.page.locator('[role="menu"],.ant-dropdown-menu')
                try:
                    scoped=unique_visible(exact(menus,preset['preset']),'菜单内对应快捷预设',True)
                    if scoped:return scoped
                    if any(m.is_visible() for m in menus.all()):return None
                    return unique_visible(exact(self.page,preset['preset']),'快捷预设',True)
                except PreparationError:return None
            item=self.wait(menu_item,'对应快捷预设菜单出现')
        self.safe();item.click()
        # Observe an applied preset, not merely a successful click return.
        after=self.wait(lambda: self._preset_changed(before,item),'预设生效')
        self.set_date(value)
        newest=unique_visible(exact(self.page,'最新创意'),'最新创意排序')
        self.safe();newest.click()
        fingerprint=None;stable_since=0
        def stable_scan():
            nonlocal fingerprint,stable_since
            if self.read_dates()!=[value,value] or any(n.is_visible() for n in self.page.locator('.ant-spin-spinning,.ant-skeleton,[aria-busy="true"]').all()):
                fingerprint=None;stable_since=0;return None
            try:
                scan=self.bridge.call('scan',self.tab_id)
            except Exception:
                return None
            if not (scan.get('newestFirst') and scan.get('cardKeys') and all(scan['cardKeys'])):
                fingerprint=None;stable_since=0;return None
            key=(scan['pageNumber'],tuple(scan['cardKeys']))
            if key!=fingerprint:fingerprint=key;stable_since=time.monotonic();return None
            return scan if time.monotonic()-stable_since>=2 else None
        second=self.wait(stable_scan,'列表及最新创意排序稳定')
        self.category(preset['category'])
        if self.read_dates()!=[value,value]:raise PreparationError('启动前日期发生变化')
        self.page.screenshot(path=str(evidence/'prepared.png'),full_page=True)
        return {'category':preset['category'],'preset':preset['preset'],'date_start':value,'date_end':value,'newestFirst':True,'pageNumber':second['pageNumber'],'cardKeys':second['cardKeys'],'preset_observation':after}

    def _preset_changed(self,before,item):
        after=self.bridge.call('context',self.tab_id)
        if after!=before:return 'visible-filter-context-changed'
        if selected(item):return 'preset-control-selected'
        return None
