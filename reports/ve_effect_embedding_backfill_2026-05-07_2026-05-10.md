# VE effect embedding dedupe backfill

Date range: 2026-05-07 to 2026-05-10

## Thresholds
- `intraday_effect_text_threshold`: 0.94
- `old_effect_text_threshold`: 0.94
- `effect_embedding_intraday_hard_threshold`: 0.95
- `effect_embedding_crossday_hard_threshold`: 0.96
- `embedding_duplicate_candidate_threshold`: 0.9
- `effect_embedding_lookback_days`: 7

## Embedding Backfill
- creative_library effect rows: 315
- updated embeddings: 315
- unique effect texts embedded: 308
- failed: 0

## Daily Effect
### 2026-05-07
- results: 40 | preserved excluded before dedupe: 0 | excluded after: 5 | syncable after: 35
- newly marked: adult=0, intraday_text=0, old_text=1, effect_embedding_hard=4, embedding_candidate_soft=3
- old text duplicate samples:
  - Retake AI Face & Selfie Editor `5b376e336d` sim=1.0 | 多场景真人照片前后对比展示 AI 修图特效 -> 多场景真人照片前后对比展示 AI 美颜特效
- hard embedding duplicate samples:
  - Retake AI Face & Selfie Editor `8cd7a3f5a7` sim=0.971 th=0.95 same_day_effect_embedding | 真人照片前后对比滑动展示 AI 自然美颜 -> 真人照片 AI 自然美颜前后对比效果展示
  - GIO - AI Photoshoot Generator `82b1e9b721` sim=0.984 th=0.95 same_day_effect_embedding | 普通自拍一键生成高质量约会肖像特效 -> 普通自拍一键生成专业级约会肖像特效
  - Pixverse:AI Video Generator `74ffa37cba` sim=0.975 th=0.96 crossday_effect_embedding | 静态照片一键生成跳舞视频特效 -> 静态照片一键上传生成动态跳舞视频
  - Retake AI Face & Selfie Editor `2f6a1f3739` sim=0.996 th=0.96 crossday_effect_embedding | 真人照片 AI 自然美颜前后对比效果展示 -> 真人照片 AI 自然美颜前后对比展示
- soft candidate samples:
  - Pixverse:AI Video Generator `991b33ac3d` sim=0.901 same_day_effect_embedding | 静态照片一键变身奇幻恶魔特效 -> 真人照片一键变身奇幻恶魔特效
  - AI Mirror: AI Photo & Video `8c18895d64` sim=0.929 crossday_effect_embedding | 单张照片一键生成九宫格多姿态表情贴纸包 -> 单张照片生成九宫格多姿势贴纸包
  - Remini - AI Photo Enhancer `a146a4a67a` sim=0.931 crossday_effect_embedding | 真人照片 AI 一键切换多种发色特效 -> 真人照片一键切换多种 AI 艺术风格特效

### 2026-05-08
- results: 37 | preserved excluded before dedupe: 0 | excluded after: 6 | syncable after: 31
- newly marked: adult=0, intraday_text=3, old_text=0, effect_embedding_hard=3, embedding_candidate_soft=3
- intraday text duplicate samples:
  - Glam AI:Video & Photo Editor `4bfe278fa7` sim=0.95 | 真人照片 AI 生成复古宝丽来明星合影特效 -> 真人自拍 AI 生成与名人合照宝丽来特效
  - Glam AI:Video & Photo Editor `0aa2ad127d` sim=0.95 | 单人照片生成与名人合影复古宝丽来特效 -> 真人自拍 AI 生成与名人合照宝丽来特效
  - Retake AI Face & Selfie Editor `2dc3312264` sim=1.0 | 真人派对场景前后对比展示 AI 修脸特效 -> 真人派对场景前后对比展示 AI 修脸特效
- hard embedding duplicate samples:
  - Retake AI Face & Selfie Editor `7a7e075f35` sim=0.981 th=0.95 same_day_effect_embedding | 真人照片前后对比展示 AI 一键美颜修复特效 -> 真人照片前后对比展示 AI 一键美颜特效
  - Retake AI Face & Selfie Editor `b670264923` sim=0.965 th=0.95 same_day_effect_embedding | 真人多场景照片前后分屏对比展示 AI 修图 -> 真人照片前后分屏对比展示 AI 一键修图
  - Retake AI Face & Selfie Editor `08c31c44ed` sim=0.974 th=0.96 crossday_effect_embedding | 真人照片前后对比展示 AI 一键美颜特效 -> 真人照片前后对比展示 AI 自然美颜特效
- soft candidate samples:
  - Remini - AI Photo Enhancer `046066943d` sim=0.916 crossday_effect_embedding | 真人照片一键变身可爱动漫角色特效 -> 真人照片一键变身动态动漫情侣特效
  - Retake AI Face & Selfie Editor `63bf95bc51` sim=0.911 crossday_effect_embedding | AI 人脸照片前后对比，一键生成更上镜版本 -> AI 人脸照片前后对比修图，一键优化上镜效果
  - Retake AI Face & Selfie Editor `9128a912b7` sim=0.93 crossday_effect_embedding | 真人照片 AI 修复前后对比，美化提升质感 -> 真人照片前后对比展示 AI 一键美化修复效果

### 2026-05-09
- results: 70 | preserved excluded before dedupe: 1 | excluded after: 12 | syncable after: 58
- newly marked: adult=0, intraday_text=3, old_text=3, effect_embedding_hard=5, embedding_candidate_soft=3
- intraday text duplicate samples:
  - Pixverse:AI Video Generator `eeec12f2d0` sim=1.0 | 静态照片一键生成跳舞动画特效 -> 静态照片一键生成跳舞动画特效
  - Pixverse:AI Video Generator `31b0a21df2` sim=1.0 | 静态照片一键生成被花朵包围的动态视频特效 -> 静态照片一键生成被花朵包围的动态视频特效
  - AI Mirror: AI Photo & Video `a18f52c970` sim=1.0 | 单张真人照片一键生成全套 3D 泡泡贴纸表情包 -> 单张真人照片一键生成全套 3D 泡泡贴纸表情包
- old text duplicate samples:
  - Pixverse:AI Video Generator `7295463a76` sim=1.0 | 静态照片一键生成跳舞动画特效 -> 静态照片一键生成跳舞视频特效
  - Pixverse:AI Video Generator `eeec12f2d0` sim=1.0 | 静态照片一键生成跳舞动画特效 -> 静态照片一键生成跳舞视频特效
  - Pixverse:AI Video Generator `f31562448f` sim=1.0 | 真人照片一键变身奇幻恶魔特效 -> 真人照片一键变身奇幻恶魔特效
  - Retake AI Face & Selfie Editor `55d29d61fa` sim=0.95 | AI 人脸修复前后对比，游艇派对场景展示自然美颜 -> 真人派对场景前后对比展示 AI 修脸特效
- hard embedding duplicate samples:
  - Pixverse:AI Video Generator `7e5b61c55d` sim=0.953 th=0.95 same_day_effect_embedding | 静态婴儿照片生成骑恐龙奇幻动画视频 -> 婴儿照片一键生成骑恐龙奇幻动画视频
  - Pixverse:AI Video Generator `8ce26887ab` sim=0.967 th=0.95 same_day_effect_embedding | 静态照片一键生成同款跳舞动画视频 -> 静态照片一键生成动态跳舞视频
  - Pixverse:AI Video Generator `52149d2ac7` sim=0.987 th=0.96 crossday_effect_embedding | 静态照片一键生成动态跳舞视频 -> 静态照片一键上传生成动态跳舞视频
  - Pixverse:AI Video Generator `df7ab893fe` sim=0.97 th=0.96 crossday_effect_embedding | 静态照片一键生成奇幻恶魔变身动态视频 -> 静态照片一键变身奇幻恶魔特效
  - Remini - AI Photo Enhancer `6e1ebee6a8` sim=0.994 th=0.96 crossday_effect_embedding | 普通自拍一键生成黑白专业模特写真特效 -> 普通自拍一键生成专业黑白模特写真
- soft candidate samples:
  - AI Mirror: AI Photo & Video `535ff5f45d` sim=0.91 same_day_effect_embedding | 宠物实拍照片一键变身蜡笔手绘风格插画 -> 真实宠物照片一键转换为蜡笔手绘风格特效
  - Pixverse:AI Video Generator `5500b5695b` sim=0.907 crossday_effect_embedding | 婴儿照片一键生成骑恐龙奇幻动画视频 -> 静态照片一键生成宝宝骑恐龙奇幻动态视频
  - Pixverse:AI Video Generator `95750a0f7b` sim=0.911 crossday_effect_embedding | 静态照片或提示词一键生成电影级动态视频 -> 静态照片一键生成电影级动作视频

### 2026-05-10
- results: 65 | preserved excluded before dedupe: 1 | excluded after: 8 | syncable after: 56
- newly marked: adult=0, intraday_text=3, old_text=2, effect_embedding_hard=2, embedding_candidate_soft=2
- intraday text duplicate samples:
  - AI Mirror: AI Photo & Video `be4a3958a9` sim=0.95 | 两张照片一键生成母亲节手绘风格合照 -> 两张独立照片一键生成母亲节手绘风格合照
  - AI Mirror: AI Photo & Video `a935fe95c3` sim=0.95 | 上传两张照片生成母亲节手绘风格合照特效 -> 两张独立照片一键生成母亲节手绘风格合照
  - AI Mirror: AI Photo & Video `3cf63eb300` sim=0.95 | 两张照片一键生成母亲节手绘风格母女合照 -> 两张独立照片一键生成母亲节手绘风格合照
- old text duplicate samples:
  - Pixverse:AI Video Generator `cfd73ccc2d` sim=1.0 | 多图融合生成奇幻骑行视频特效 -> 多图融合生成奇幻骑乘视频特效
  - Pixverse:AI Video Generator `faa3f9bb94` sim=1.0 | 静态照片一键生成同步舞蹈动画视频 -> 静态照片一键生成同步舞蹈动画
- hard embedding duplicate samples:
  - AI Mirror: AI Photo & Video `cab8a70bec` sim=0.959 th=0.95 same_day_effect_embedding | 两张照片一键 AI 融合生成温馨合影特效 -> 两张照片一键 AI 融合生成和谐双人合影特效
  - Retake AI Face & Selfie Editor `af0011c6b9` sim=0.979 th=0.96 crossday_effect_embedding | 多场景真人照片 AI 自然美颜前后对比展示 -> 多场景真人照片前后对比展示 AI 美颜特效
- soft candidate samples:
  - AI Mirror: AI Photo & Video `b3178c6528` sim=0.912 same_day_effect_embedding | 真人照片一键生成手绘艺术风格特效 -> 真人照片一键生成手绘素描风格 AI 特效
  - AI Mirror: AI Photo & Video `74e3a29984` sim=0.922 same_day_effect_embedding | 两张单人照 AI 融合生成温馨合影特效 -> 两张照片一键 AI 融合生成和谐双人合影特效
