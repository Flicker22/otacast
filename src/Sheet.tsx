import { useEffect, useRef, type ReactNode } from "react";

interface SheetProps {
  id: string;
  title: string;
  description: string;
  open: boolean;
  onClose: () => void;
  children: ReactNode;
}

export function Sheet({ id, title, description, open, onClose, children }: SheetProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!open || !dialog) return;
    dialog.showModal();
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      dialog.close();
      document.body.style.overflow = previousOverflow;
    };
  }, [open]);

  return (
    <dialog ref={dialogRef} id={id} className="sheet" aria-labelledby={`${id}-title`} aria-describedby={`${id}-description`} onCancel={(event) => { event.preventDefault(); onClose(); }}>
      <div className="sheet-header">
        <div>
          <h2 id={`${id}-title`}>{title}</h2>
          <p id={`${id}-description`}>{description}</p>
        </div>
        <button type="button" className="btn btn-secondary sheet-close" onClick={onClose} aria-label={`关闭${title}`} autoFocus>关闭</button>
      </div>
      {children}
    </dialog>
  );
}
