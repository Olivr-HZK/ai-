"""One display filename for selected-media uploads and ZIP members."""
from datetime import date
import re
import unicodedata

NAMING_VERSION = 'date-sequence-advertiser-v1'


def media_stem(target_date, sequence, advertiser):
    day = date.fromisoformat(target_date).isoformat()
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise ValueError('Media sequence must be a positive selection rank')
    if not isinstance(advertiser, str) or not advertiser.strip():
        raise ValueError('Observed advertiser name required')
    name = unicodedata.normalize('NFC', advertiser)
    name = re.sub(r'[\x00-\x1f\x7f<>:"/\\|?*]', '_', name).strip(' .')
    # Leave room for the date, sequence, optional image part and extension.
    name = name.encode('utf-8')[:180].decode('utf-8', errors='ignore').rstrip(' .')
    if not name:
        raise ValueError('Advertiser has no usable filename characters')
    return f'{day}-{sequence:03d}-{name}'


def media_filename(target_date, sequence, advertiser, suffix, part=None):
    suffix = suffix.lower()
    if not re.fullmatch(r'\.[a-z0-9]{1,8}', suffix):
        raise ValueError('Original media file extension required')
    stem = media_stem(target_date, sequence, advertiser)
    if part is not None:
        if isinstance(part, bool) or not isinstance(part, int) or part < 1:
            raise ValueError('Image part must be a positive integer')
        stem += f'-{part:02d}'
    return stem + suffix
