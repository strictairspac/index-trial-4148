"""Six-decimal canonicalisation shared by ingestion, correction and readback."""
from decimal import Decimal,InvalidOperation,localcontext
SIX=Decimal('0.000001')
ZERO=Decimal('0.000000')
DECIMAL_LIMIT=Decimal(10)**42          # DECIMAL(48,6) allows 42 integer digits
# DECIMAL(48,6) needs up to 48 significant digits; the default context caps at 28,
# which would spuriously reject a legitimate 45-digit reading during quantize.
_CONTEXT_PRECISION=60
def parse_six(raw):
 """Return a Decimal quantised to exactly six places, or raise ValueError."""
 if isinstance(raw,bool) or raw is None:raise ValueError('数值格式无效')
 try:value=Decimal(str(raw).strip())
 except (InvalidOperation,ValueError):raise ValueError('数值格式无效')
 if not value.is_finite():raise ValueError('数值必须为有限小数')
 # copy_abs() ignores the decimal context; abs() would round a 48-digit value
 # up to 1E+42 under the default 28-digit precision and falsely reject it.
 if value.copy_abs()>=DECIMAL_LIMIT:raise ValueError('数值超出可存储范围')
 with localcontext() as ctx:
  ctx.prec=_CONTEXT_PRECISION
  try:quantised=value.quantize(SIX)
  except InvalidOperation:raise ValueError('数值超出可存储范围')
 # compare() is context-free; != would round 48-digit operands under the default
 # 28-digit precision and wrongly report a precision loss.
 if quantised.compare(value)!=0:raise ValueError('最多支持六位小数')
 return ZERO if quantised==0 else quantised
def fmt_six(value):
 """Format a Decimal/str as fixed six-decimal text."""
 try:
  with localcontext() as ctx:
   ctx.prec=_CONTEXT_PRECISION
   return format(Decimal(str(value)).quantize(SIX),'f')
 except (InvalidOperation,ValueError):return str(value)
