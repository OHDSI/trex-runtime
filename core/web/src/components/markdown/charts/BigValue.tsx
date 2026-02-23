interface BigValueProps {
  title?: string;
  value: string;
  fmt?: string;
  comparison?: string;
  delta?: string;
  downIsGood?: boolean;
}

export function BigValue({
  title,
  value,
  fmt,
  comparison,
  delta,
  downIsGood,
}: BigValueProps) {
  const display = fmt ? `${fmt}${value}` : value;

  const deltaNum = delta ? parseFloat(delta) : null;
  let deltaColor = "text-muted-foreground";
  let deltaArrow = "";
  if (deltaNum !== null && deltaNum !== 0) {
    const isPositive = deltaNum > 0;
    const isGood = downIsGood ? !isPositive : isPositive;
    deltaColor = isGood ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400";
    deltaArrow = isPositive ? "\u25B2 " : "\u25BC ";
  }

  return (
    <div className="inline-block font-sans pt-2 pb-3 pl-0 mr-3 items-center align-top min-w-[18%]">
      {title && (
        <p className="text-sm align-top leading-none">{title}</p>
      )}
      <div className="relative text-xl font-medium mt-1.5">{display}</div>
      {(comparison || delta) && (
        <p className={`text-xs font-sans mt-1 ${deltaColor}`}>
          {delta && (
            <span className="font-[system-ui]">{deltaArrow}</span>
          )}
          {delta ?? ""}{comparison ? ` ${comparison}` : ""}
        </p>
      )}
    </div>
  );
}
