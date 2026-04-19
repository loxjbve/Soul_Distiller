# 修复 Persona Studio 布局与支持中英文切换 Spec

## Why
在项目控制台中，Persona Studio 面板内的卡片（如“Select target user”、“Fill persona context”）出现了高度压缩和相互重叠的问题，导致底部的分析等操作按钮被遮挡且无法点击。此外，系统目前缺乏全局的中英文切换机制，所有界面均为硬编码或单一语言，无法满足双语环境用户的需求。

## What Changes
- 修复 `.persona-stage-stack` 和 `.persona-stage-card` 相关的 CSS 布局，消除 flex 子项因为空间不足而被压缩或覆盖堆叠的问题，确保卡片内容完整显示并提供必要的内部滚动能力。
- 在后端引入多语言支持，通过 Cookie (`locale`) 存储并传递用户的语言偏好。
- 提取并完善 `app/web/ui_strings.py` 中的中英文（`zh-CN`, `en-US`）翻译词典。
- 在顶部导航栏（`base.html`）添加语言切换开关，支持一键切换并自动刷新页面，将全站所有可见的硬编码文本替换为多语言变量渲染。

## Impact
- Affected specs: UI 布局，国际化 (i18n)
- Affected code: `app/static/pages.css`, `app/web/ui_strings.py`, `app/web/routes.py`, `app/templates/base.html` 及所有需要本地化的模板文件。

## ADDED Requirements
### Requirement: 中英文切换功能
系统 SHALL 提供中英双语切换能力，并全局生效。

#### Scenario: 成功切换语言
- **WHEN** 用户点击导航栏的语言切换按钮（“中文/En”）
- **THEN** 调用切换接口，界面重新加载，所有文本替换为所选语言，且偏好被记录在 Cookie 中，后续访问维持所选语言。

## MODIFIED Requirements
### Requirement: Persona Studio 布局
Persona Studio 内部的向导卡片 SHALL 正常铺展，禁止出现相互遮挡。如果内容过长，允许内部滚动，且保证所有操作按钮均可被用户正常访问和点击。
